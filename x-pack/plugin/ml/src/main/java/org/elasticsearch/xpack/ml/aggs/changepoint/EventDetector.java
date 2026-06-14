/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Top-level entry point. Runs three independent detectors over the series and merges their events:
 * <ul>
 *   <li>structural step/trend changes on the values;</li>
 *   <li>distribution step/trend changes, found as structural changes on a windowed dispersion channel
 *       and relabelled as {@link ChangeType.DistributionChange};</li>
 *   <li>spikes/dips, found as point excursions from a local baseline, but checked for global rarity.</li>
 * </ul>
 *
 * Structural value and distribution events are de-duplicated against each other, so a regime boundary
 * that shifts both level and spread is reported once. Pulses are a separate stream and may legitimately
 * coincide with a structural boundary, so they are added after de-duplication. All events are finally
 * mapped back to source-bucket indices.
 */
public class EventDetector {

    private static final Logger logger = LogManager.getLogger(EventDetector.class);

    public EventDetector() {
        this(MIN_SEGMENT_LENGTH, P_VALUE_THRESHOLD);
    }

    public EventDetector(int minSegmentLength) {
        this(minSegmentLength, P_VALUE_THRESHOLD);
    }

    public EventDetector(int minSegmentLength, double pValueThreshold) {
        this.minSegmentLength = minSegmentLength;
        this.dispersionMinSegment = Math.max(3, minSegmentLength / DISPERSION_WINDOW);
        this.pulseDetector = new PulseDetector(minSegmentLength, pValueThreshold);
        this.detectorForValues = new StructuralChangeDetector(minSegmentLength, VALUE_MAX_DEGREE, pValueThreshold);
        this.detectorForDispersions = new StructuralChangeDetector(dispersionMinSegment, DISPERSION_MAX_DEGREE, pValueThreshold);
    }

    public List<ChangeType> detect(MlAggsHelper.DoubleBucketValues bucketValues) {
        if (bucketValues.getValues().length < (2 * minSegmentLength) + 2) {
            return List.of(
                new ChangeType.Indeterminable(
                    "not enough buckets to calculate change_point. Requires at least ["
                        + ((2 * minSegmentLength) + 2)
                        + "]; found ["
                        + bucketValues.getValues().length
                        + "]"
                )
            );
        }

        MlAggsHelper.DoubleBucketValues sampledBucketValues = SeriesDownsampler.downsample(bucketValues, MAX_SAMPLES);
        double[] values = sampledBucketValues.getValues();

        List<ChangeType> events = new ArrayList<>();

        // Distribution (variance) changes: structural changes on the dispersion channel, relabelled and remapped
        // from channel-sample space to value-index space.
        double[] dispersion = Stats.windowedDispersion(values, dispersionMinSegment, DISPERSION_WINDOW);
        if (dispersion != null) {
            for (ChangeType e : detectorForDispersions.detect(dispersion)) {
                if (e.isChange() == false) {
                    continue;
                }
                // For window length w, the channel change-point k = first sample of the new regime, centred
                // at value index w * k + w / 2; the previous sample is centred at w * k - w / 2, so the regime
                // boundary sits between them, at value index w * k.
                int valueIndex = Math.min(values.length - 1, DISPERSION_WINDOW * e.changePoint());
                events.add(new ChangeType.DistributionChange(e.pValue(), valueIndex));
            }
        }

        // Structural (step/trend) changes on the values.
        events.addAll(detectorForValues.detect(values));

        // Collapse a distribution/structural pair marking the same regime boundary into the more significant one;
        // pulses are a separate stream and are added afterwards so they are never suppressed by a coincident break.
        events = deduplicate(events);

        // Anomalies (spikes/dips): point excursions from a local baseline, judged against a KDE of the background
        // values with the detected excursions removed.
        events.addAll(pulseDetector.detect(values));

        // If any changes spikes or dips exist remove the stationary/non-stationary classification.
        if (events.stream().anyMatch(ChangeType::isChange)) {
            events.removeIf(e -> !e.isChange());
        }

        events.sort(Comparator.comparingInt(ChangeType::changePoint));

        // Map value-array indices back to source buckets. Stationary/non-stationary classifications carry no
        // index (NO_CHANGE_POINT), so they are passed through unremapped.
        return events.stream()
            .map(e -> e.isChange() ? e.remapChangePoint(sampledBucketValues.getBucketIndex(e.changePoint())) : e)
            .toList();
    }

    /**
     * Within any cluster of events falling within minSegmentLength of one another, keeps only the most significant
     * (smallest p-value), so a single regime boundary is not reported by more than one detector.
     */
    private List<ChangeType> deduplicate(List<ChangeType> events) {
        Integer[] order = new Integer[events.size()];
        for (int i = 0; i < order.length; i++) {
            order[i] = i;
        }
        java.util.Arrays.sort(order, Comparator.comparingDouble(i -> events.get(i).pValue()));

        List<ChangeType> kept = new ArrayList<>();
        List<ChangeType> classification = new ArrayList<>();
        for (int i : order) {
            if (events.get(i).isChange() == false) {
                classification.add(events.get(i));
                continue;
            }
            int cp = events.get(i).changePoint();
            boolean tooClose = false;
            for (ChangeType k : kept) {
                if (Math.abs(cp - k.changePoint()) < minSegmentLength) {
                    logger.trace(
                        "suppressing event at index [{}] with p-value [{}] because it is within [{}] of more significant event at index [{}]",
                        cp,
                        events.get(i).pValue(),
                        minSegmentLength,
                        k.changePoint()
                    );
                    tooClose = true;
                    break;
                }
            }
            if (tooClose == false) {
                kept.add(events.get(i));
            }
        }
        return kept.isEmpty() ? classification : kept;
    }

    // Runtime safeguard: detection cost grows with the bucket count (PELT's reset loop), so a series longer than
    // this is collapsed by SeriesDownsampler before any detection. The downsampled series carries the original
    // bucket indices, so events remap back to source buckets via getBucketIndex; below the cap it is a no-op.
    private static final int MAX_SAMPLES = 2000;
    // The minimum number of buckets between two structural changes for them to be reported as distinct events.
    // Anything shorter is considered a spike or dip.
    private static final int MIN_SEGMENT_LENGTH = 10;
    // The threshold for the p-value to emit a change point and spike and dip. P-values are Bonferroni-corrected
    // by the number of candidate events and the threshold is applied to the corrected value.
    private static final double P_VALUE_THRESHOLD = 0.01;
    // The dispersion channel holds one sample per this many points (non-overlapping windows). A channel sample is
    // centred on the middle of its window, so channel index k corresponds to value index window/2 + window * k.
    private static final int DISPERSION_WINDOW = 8;
    // Because we downsample the dispersion channel it has significantly fewer values original time series. To
    // compensate we also use shorter segments. This means high degree trends overfit and we limit to linear only.
    private static final int DISPERSION_MAX_DEGREE = 1;
    private static final int VALUE_MAX_DEGREE = 3;

    private final int minSegmentLength;
    private final int dispersionMinSegment;
    private final PulseDetector pulseDetector;
    private final StructuralChangeDetector detectorForValues;
    private final StructuralChangeDetector detectorForDispersions;
}
