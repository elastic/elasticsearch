/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.test.knn.KnnIndexTester.logger;

/**
 * Generates synthetic multi-tenant vector data for KNN benchmarking.
 * Documents are distributed across tenants according to a configurable distribution,
 * and each document is assigned a tenant_id and a random vector.
 */
class MultiTenantDataGenerator {

    private final int numDocs;
    private final int dimensions;
    private final int numTenants;
    private final String distribution;
    private final Random random;
    private final Map<String, List<Integer>> tenantAssignments;

    MultiTenantDataGenerator(int numDocs, int dimensions, int numTenants, String distribution, long seed) {
        this.numDocs = numDocs;
        this.dimensions = dimensions;
        this.numTenants = numTenants;
        this.distribution = distribution;
        this.random = new Random(seed);
        this.tenantAssignments = computeTenantAssignments();
    }

    /**
     * Returns the tenant_id for a given document ordinal.
     */
    String tenantId(int docOrd) {
        // Iterate through tenant assignments to find the tenant for this doc
        for (var entry : tenantAssignments.entrySet()) {
            if (entry.getValue().contains(docOrd)) {
                return entry.getKey();
            }
        }
        throw new IllegalStateException("doc " + docOrd + " not assigned to any tenant");
    }

    /**
     * Returns the tenant assignments: a map from tenant_id to the list of document ordinals assigned to that tenant.
     * Documents are assigned contiguously per tenant (sorted by tenant_id) to facilitate index sorting.
     */
    Map<String, List<Integer>> getTenantAssignments() {
        return tenantAssignments;
    }

    /**
     * Returns the number of tenants.
     */
    int getNumTenants() {
        return numTenants;
    }

    /**
     * Generates a random float vector of the configured dimensions.
     */
    float[] nextVector() {
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = random.nextFloat() * 2 - 1; // uniform in [-1, 1]
        }
        return vector;
    }

    /**
     * Generates a random byte vector of the configured dimensions.
     */
    byte[] nextByteVector() {
        byte[] vector = new byte[dimensions];
        random.nextBytes(vector);
        return vector;
    }

    /**
     * Generates query vectors. These are random vectors not tied to any tenant.
     */
    float[][] generateQueryVectors(int numQueries) {
        float[][] queries = new float[numQueries][dimensions];
        for (int i = 0; i < numQueries; i++) {
            queries[i] = nextVector();
        }
        return queries;
    }

    /**
     * Generates query byte vectors. These are random vectors not tied to any tenant.
     */
    byte[][] generateQueryByteVectors(int numQueries) {
        byte[][] queries = new byte[numQueries][dimensions];
        for (int i = 0; i < numQueries; i++) {
            queries[i] = nextByteVector();
        }
        return queries;
    }

    private Map<String, List<Integer>> computeTenantAssignments() {
        int[] docsPerTenant = computeDocsPerTenant();
        Map<String, List<Integer>> assignments = new LinkedHashMap<>();
        int docOrd = 0;
        for (int t = 0; t < numTenants; t++) {
            String tenantId = String.format("tenant_%06d", t);
            List<Integer> docIds = new ArrayList<>(docsPerTenant[t]);
            for (int d = 0; d < docsPerTenant[t]; d++) {
                docIds.add(docOrd++);
            }
            assignments.put(tenantId, docIds);
        }
        logger.info("Generated tenant assignments: {} tenants, {} total docs, distribution={}", numTenants, docOrd, distribution);
        return assignments;
    }

    private int[] computeDocsPerTenant() {
        return switch (distribution) {
            case "uniform" -> computeUniform();
            case "zipf" -> computeZipf();
            default -> throw new IllegalArgumentException("Unknown tenant distribution: " + distribution + ". Use 'uniform' or 'zipf'.");
        };
    }

    private int[] computeUniform() {
        int[] counts = new int[numTenants];
        int base = numDocs / numTenants;
        int remainder = numDocs % numTenants;
        for (int i = 0; i < numTenants; i++) {
            counts[i] = base + (i < remainder ? 1 : 0);
        }
        return counts;
    }

    /**
     * Computes a Zipf distribution of documents across tenants.
     * The first tenant gets the most documents, following a power-law decay.
     */
    private int[] computeZipf() {
        double[] weights = new double[numTenants];
        double totalWeight = 0;
        for (int i = 0; i < numTenants; i++) {
            weights[i] = 1.0 / (i + 1); // Zipf: rank^-1
            totalWeight += weights[i];
        }
        int[] counts = new int[numTenants];
        int assigned = 0;
        for (int i = 0; i < numTenants; i++) {
            counts[i] = Math.max(1, (int) Math.round(numDocs * weights[i] / totalWeight));
            assigned += counts[i];
        }
        // Adjust for rounding errors
        int diff = numDocs - assigned;
        if (diff > 0) {
            // distribute remaining docs to the largest tenants
            for (int i = 0; i < diff; i++) {
                counts[i % numTenants]++;
            }
        } else if (diff < 0) {
            // remove excess docs from the smallest tenants (from the end)
            for (int i = numTenants - 1; i >= 0 && diff < 0; i--) {
                int remove = Math.min(-diff, counts[i] - 1);
                counts[i] -= remove;
                diff += remove;
            }
        }
        return counts;
    }
}
