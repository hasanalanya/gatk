package org.broadinstitute.hellbender.tools.walkers.annotator;


import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.gatk.nativebindings.smithwaterman.SWOverhangStrategy;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.genotyper.AlleleLikelihoods;
import org.broadinstitute.hellbender.utils.haplotype.EventMap;
import org.broadinstitute.hellbender.utils.haplotype.Haplotype;
import org.broadinstitute.hellbender.utils.help.HelpConstants;
import org.broadinstitute.hellbender.utils.read.AlignmentUtils;
import org.broadinstitute.hellbender.utils.read.CigarUtils;
import org.broadinstitute.hellbender.utils.read.Fragment;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.smithwaterman.SmithWatermanAligner;
import org.broadinstitute.hellbender.utils.smithwaterman.SmithWatermanAlignment;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 */
@DocumentedFeature(groupName= HelpConstants.DOC_CAT_ANNOTATORS, groupSummary=HelpConstants.DOC_CAT_ANNOTATORS_SUMMARY,
        summary="Describe the complexity of an assembly region")
public class AssemblyComplexity extends JumboInfoAnnotation {

    public AssemblyComplexity() { }

    @Override
    public Map<String, Object> annotate(final ReferenceContext ref,
                                                 final FeatureContext features,
                                                 final VariantContext vc,
                                                 final AlleleLikelihoods<GATKRead, Allele> likelihoods,
                                                 final AlleleLikelihoods<Fragment, Allele> fragmentLikelihoods,
                                                 final AlleleLikelihoods<Fragment, Haplotype> haplotypeLikelihoods) {

        final Map<String, Object> result = new HashMap<>();

        // count best-read support for each haplotype
        final Map<Haplotype, MutableInt> haplotypeSupportCounts = haplotypeLikelihoods.alleles().stream()
                .collect(Collectors.toMap(hap -> hap, label -> new MutableInt(0)));
        haplotypeLikelihoods.bestAllelesBreakingTies()
                .forEach(bestHaplotype -> haplotypeSupportCounts.get(bestHaplotype.allele).increment());

        // encode each haplotype as a string of variant starts and alt allele strings, excluding the locus of vc
        // note that VariantContexts in an EventMap are always biallelic, so var.getAlternateAllele(0) is valid
        final Map<String, List<Haplotype>> haplotypeGroups = haplotypeLikelihoods.alleles().stream()
                .collect(Collectors.groupingBy(hap -> hap.getEventMap().getVariantContexts().stream()
                        .filter(var -> var.getStart() != vc.getStart())
                        .map(var -> var.getStart() + var.getAlternateAllele(0).getBaseString())
                        .collect(Collectors.joining())));

        // sum the read support counts for all haplotypes within each group
        final int[] equivalenceCounts = haplotypeGroups.values().stream()
                .mapToInt(haps -> haps.stream().map(haplotypeSupportCounts::get).mapToInt(MutableInt::intValue).sum())
                .toArray();

        result.put(GATKVCFConstants.HAPLOTYPE_EQUIVALENCE_COUNTS_KEY, equivalenceCounts);

        // we're going to calculate the complexity of this variant's haplotype (that is, the variant-supporting haplotype
        // with the most reads) versus the closest (in terms of edit distance) germline haplotype.  The haplotype
        // with the greatest read support is considered germline, and as a heuristic we consider the second-most-supported
        // haplotype to be germline as well if it is at least half as supported (in terms of best read count) as the most-supported.
        final List<Haplotype> haplotypesByDescendingSupport = haplotypeSupportCounts.entrySet().stream()
                .sorted(Comparator.comparingInt(entry -> -entry.getValue().intValue()))
                .map(entry -> entry.getKey())
                .collect(Collectors.toList());

        final List<Haplotype> germlineHaplotypes = new ArrayList<>();
        germlineHaplotypes.add(haplotypesByDescendingSupport.get(0));
        if (haplotypesByDescendingSupport.size() > 1 && haplotypeSupportCounts.get(haplotypesByDescendingSupport.get(1)).intValue() >= haplotypeSupportCounts.get(haplotypesByDescendingSupport.get(0)).intValue()/2) {
            germlineHaplotypes.add(haplotypesByDescendingSupport.get(1));
        }

        final int[] editDistances = vc.getAlternateAlleles().stream().mapToInt(allele -> {
            final Haplotype mostSupportedHaplotypeWithAllele = haplotypesByDescendingSupport.stream()
                    .filter(hap -> containsAllele(hap.getEventMap(), allele, vc.getStart()))
                    .findFirst().get();

            return germlineHaplotypes.stream().mapToInt(gh -> editDistance(gh, mostSupportedHaplotypeWithAllele)).min().getAsInt();
        }).toArray();

        result.put(GATKVCFConstants.HAPLOTYPE_COMPLEXITY_KEY, editDistances);

        return result;
    }


    @Override
    public List<String> getKeyNames() {
        return Arrays.asList(GATKVCFConstants.HAPLOTYPE_EQUIVALENCE_COUNTS_KEY, GATKVCFConstants.HAPLOTYPE_COMPLEXITY_KEY);
    }

    // does an EventMap contain a variant allele from another EventMap.
    // we assume that everything is derived from a common assembly and thus variant start positions
    // are consistent
    private static boolean containsAllele(final EventMap eventMap, final Allele variantAllele, final int position) {
        final List<VariantContext> overlapping =  eventMap.getOverlappingEvents(position);
        return !overlapping.isEmpty() && overlapping.get(0).getAlternateAllele(0).basesMatch(variantAllele);
    }

    // count variants in one haplotype but not the other
    // note that we use the fact that EventMap VariantContexts are biallelic
    private static int uniqueVariants(final Haplotype hap1, final Haplotype hap2) {
        final EventMap eventMap2 = hap2.getEventMap();
        return (int) hap1.getEventMap().getVariantContexts().stream()
                .filter(vc -> !containsAllele(eventMap2, vc.getAlternateAllele(0), vc.getStart()))
                .count();
    }

    private static int editDistance(final Haplotype hap1, final Haplotype hap2) {
        return uniqueVariants(hap1, hap2) + uniqueVariants(hap2, hap1);
    }

}
