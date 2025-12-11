/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.HnswQueueSaturationCollector;
import org.apache.lucene.search.KnnCollector;

/**
 * A {@link KnnCollector.Decorator} extending {@link HnswQueueSaturationCollector}
 * that adaptively early-exits HNSW search using an online-estimated discovery rate,
 * rolling mean/variance, and adaptive patience threshold.
 * It tracks smoothed discovery rate (how many new neighbors are collected per candidate),
 * maintains a rolling mean and variance of the rate (using Welford's algorithm).
 * Those are used to define an adaptive saturation threshold = mean + looseness * stddev
 * and adaptive patience = patience-scaling / (1 + stddev).
 * Adaptive patience scales inversely with volatility (stddev) and looseness.
 * Patience-scaling defines patience order of magnitude.
 * Saturation happens when the discovery rate is lower than the adaptive saturation threshold.
 * The collector early exits once saturation persists for longer than adaptive patience.
 */
public class AdaptiveHnswQueueSaturationCollector extends HnswQueueSaturationCollector {

    private static final float DEFAULT_DISCOVERY_RATE_SMOOTHING = 0.9f;
    private static final float DEFAULT_THRESHOLD_LOOSENESS = 0.01f;
    private static final float DEFAULT_PATIENCE_SCALING = 10.0f;

    private final float discoveryRateSmoothing;
    private final float thresholdLooseness;
    private final float patienceScaling;

    private final KnnCollector delegate;
    private boolean patienceFinished = false;

    private int previousQueueSize = 0;
    private int currentQueueSize = 0;

    private float smoothedDiscoveryRate = 0.0f;
    private float mean = 0.0f;
    private float m2 = 0.0f;
    private int samples = 0;
    private int steps = 0;

    private int saturatedCount = 0;

    private AdaptiveHnswQueueSaturationCollector(
        KnnCollector delegate,
        float discoveryRateSmoothing,
        float thresholdLooseness,
        float patienceScaling
    ) {
        super(delegate, 0, 0);
        this.delegate = delegate;
        this.discoveryRateSmoothing = discoveryRateSmoothing;
        this.thresholdLooseness = thresholdLooseness;
        this.patienceScaling = patienceScaling;
    }

    public AdaptiveHnswQueueSaturationCollector(KnnCollector delegate) {
        this(delegate, DEFAULT_DISCOVERY_RATE_SMOOTHING, DEFAULT_THRESHOLD_LOOSENESS, DEFAULT_PATIENCE_SCALING);
    }

    @Override
    public boolean earlyTerminated() {
        return patienceFinished || delegate.earlyTerminated();
    }

    @Override
    public boolean collect(int docId, float similarity) {
        boolean collected = delegate.collect(docId, similarity);
        if (collected) {
            currentQueueSize++;
        }
        steps++;
        return collected;
    }

    @Override
    public void nextCandidate() {
        // rate of newly discovered neighbors for the current candidate
        float discoveryRate = (float) ((currentQueueSize - previousQueueSize) / (1e-9 + steps * k()));
        float rate = Math.max(0, discoveryRate);

        // exponentially smoothed discovery rate
        smoothedDiscoveryRate = discoveryRateSmoothing * rate + (1 - discoveryRateSmoothing) * smoothedDiscoveryRate;

        // update rolling mean and variance using Welford's algorithm
        samples++;
        float deltaMean = smoothedDiscoveryRate - mean;
        mean += deltaMean / samples;
        m2 += deltaMean * (smoothedDiscoveryRate - mean);
        double variance = samples > 1 ? m2 / (samples - 1) : 0.0;
        double stddev = Math.sqrt(variance);

        // update adaptive threshold and patience
        double adaptiveThreshold = mean + thresholdLooseness * stddev;
        double adaptivePatience = patienceScaling / (1.0 + stddev);

        if (smoothedDiscoveryRate < adaptiveThreshold) {
            saturatedCount++;
        } else {
            saturatedCount = 0;
        }

        if (saturatedCount > adaptivePatience) {
            patienceFinished = true;
        }

        previousQueueSize = currentQueueSize;
        steps = 0;
    }

}
