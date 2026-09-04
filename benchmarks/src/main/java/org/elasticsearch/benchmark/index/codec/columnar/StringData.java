/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.util.BytesRef;

import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * Keyword column shapes, spanning the range that decides how a string column is stored: a handful of
 * distinct values, a moderate vocabulary with a skewed distribution, a large one drawn uniformly, and a
 * column where nearly every value is distinct.
 */
public enum StringData {

    /** A log level: a few values, heavily skewed. Every row is covered by a tiny dictionary. */
    LOG_LEVEL {
        @Override
        BytesRef[] generate(int count, Random random) {
            final String[] vocabulary = { "INFO", "DEBUG", "WARN", "ERROR", "TRACE" };
            return skewed(vocabulary, count, random, 2.0);
        }
    },

    /** A host name: thousands of values sharing long prefixes and suffixes, skewed towards a few hosts. */
    HOSTNAME {
        @Override
        BytesRef[] generate(int count, Random random) {
            final String[] vocabulary = new String[5_000];
            for (int i = 0; i < vocabulary.length; i++) {
                vocabulary[i] = "ip-10-" + (i / 254 % 256) + "-" + (i % 254 + 1) + ".eu-west-1.compute.internal";
            }
            return skewed(vocabulary, count, random, 1.5);
        }
    },

    /** A pod name: a large vocabulary drawn uniformly, so no dictionary of any size covers much of it. */
    POD_NAME {
        @Override
        BytesRef[] generate(int count, Random random) {
            final String[] vocabulary = new String[50_000];
            for (int i = 0; i < vocabulary.length; i++) {
                vocabulary[i] = "checkout-7d9f8b6c4-" + Integer.toString(i, 36) + "xk";
            }
            return skewed(vocabulary, count, random, 0.0);
        }
    },

    /** An IP address: short, and near enough all distinct. */
    IP_ADDRESS {
        @Override
        BytesRef[] generate(int count, Random random) {
            final BytesRef[] values = new BytesRef[count];
            for (int i = 0; i < count; i++) {
                final int v = random.nextInt();
                values[i] = bytes("10." + (v >>> 16 & 0xff) + "." + (v >>> 8 & 0xff) + "." + (v & 0xff));
            }
            return values;
        }
    },

    /** ClickBench {@code HitColor}: a single character drawn from a handful of values. */
    HIT_COLOR {
        @Override
        BytesRef[] generate(int count, Random random) {
            return skewed(new String[] { "a", "b", "c", "d", "e", "f", "0", "5" }, count, random, 1.5);
        }
    },

    /**
     * ClickBench {@code SearchPhrase}: empty in most rows, with a long tail of distinct phrases. A
     * dictionary holding the empty string alone already covers most of the column.
     */
    MOSTLY_EMPTY {
        @Override
        BytesRef[] generate(int count, Random random) {
            final BytesRef[] values = new BytesRef[count];
            final BytesRef empty = bytes("");
            for (int i = 0; i < count; i++) {
                values[i] = random.nextDouble() < 0.88 ? empty : bytes("search phrase " + random.nextInt(count / 4 + 1));
            }
            return values;
        }
    },

    /** ClickBench {@code URL}: long, near-unique, and sharing prefixes. */
    URL {
        @Override
        BytesRef[] generate(int count, Random random) {
            final String[] hosts = { "www.example.com", "shop.example.org", "news.example.net", "m.example.co" };
            final BytesRef[] values = new BytesRef[count];
            for (int i = 0; i < count; i++) {
                values[i] = bytes(
                    "http://"
                        + hosts[random.nextInt(hosts.length)]
                        + "/catalog/"
                        + random.nextInt(50_000)
                        + "/item?id="
                        + random.nextInt(count)
                        + "&ref=search"
                );
            }
            return values;
        }
    },

    /**
     * Textbench {@code ServiceName} on an index sorted by it: a handful of values, in runs. Clustering this
     * strong is the case a per-page dictionary exploits best.
     */
    CLUSTERED_SERVICE {
        @Override
        BytesRef[] generate(int count, Random random) {
            final String[] services = {
                "frontend",
                "cartservice",
                "checkoutservice",
                "paymentservice",
                "shippingservice",
                "emailservice",
                "recommendationservice",
                "productcatalogservice" };
            // An index sorted by this field hands the values over in term order, not in whatever order
            // they happen to be listed above.
            java.util.Arrays.sort(services);
            final BytesRef[] values = new BytesRef[count];
            int service = 0;
            int remaining = 0;
            for (int i = 0; i < count; i++) {
                if (remaining == 0) {
                    remaining = count / services.length;
                    service = Math.min(service + 1, services.length - 1);
                }
                remaining--;
                values[i] = bytes(services[service]);
            }
            return values;
        }
    },

    /**
     * A host name on an index sorted by it: thousands of values, each in one run, in term order. This is
     * the shape a primary sort produces, and the one where a filter can find its documents by bisection
     * rather than by comparing every value.
     */
    SORTED_HOSTNAME {
        @Override
        BytesRef[] generate(int count, Random random) {
            final String[] vocabulary = new String[5_000];
            for (int i = 0; i < vocabulary.length; i++) {
                vocabulary[i] = "ip-10-" + (i / 254 % 256) + "-" + (i % 254 + 1) + ".eu-west-1.compute.internal";
            }
            java.util.Arrays.sort(vocabulary);
            final BytesRef[] values = new BytesRef[count];
            for (int i = 0; i < count; i++) {
                // Runs of equal length, so every host holds the same share of the column.
                values[i] = bytes(vocabulary[Math.min((int) ((long) i * vocabulary.length / count), vocabulary.length - 1)]);
            }
            return values;
        }
    },

    /**
     * A pod name on an index sorted by it: the same large vocabulary as {@link #POD_NAME}, in term order,
     * each name in one run. A vocabulary this large is beyond what a bounded dictionary can hold, so the
     * column stays plain — but the values arrive in runs, which is the shape both the compressor and the
     * bisection over a sorted column exploit best.
     */
    SORTED_POD_NAME {
        @Override
        BytesRef[] generate(int count, Random random) {
            final String[] vocabulary = new String[50_000];
            for (int i = 0; i < vocabulary.length; i++) {
                vocabulary[i] = "checkout-7d9f8b6c4-" + Integer.toString(i, 36) + "xk";
            }
            java.util.Arrays.sort(vocabulary);
            final BytesRef[] values = new BytesRef[count];
            for (int i = 0; i < count; i++) {
                values[i] = bytes(vocabulary[Math.min((int) ((long) i * vocabulary.length / count), vocabulary.length - 1)]);
            }
            return values;
        }
    },

    /**
     * A pod name on an index sorted by something else first — a region, say, and then the pod name and its
     * timestamp. The values arrive in runs as they would under a primary sort, but the runs restart with
     * every region, so the column as a whole never becomes ordered. It is the shape a secondary sort key
     * takes, and it separates what a compressor gets from runs of equal values from what a search gets
     * from a column it can bisect.
     */
    CLUSTERED_POD_NAME {
        @Override
        BytesRef[] generate(int count, Random random) {
            final int regions = 8;
            final String[] vocabulary = new String[50_000];
            for (int i = 0; i < vocabulary.length; i++) {
                vocabulary[i] = "checkout-7d9f8b6c4-" + Integer.toString(i, 36) + "xk";
            }
            java.util.Arrays.sort(vocabulary);
            final BytesRef[] values = new BytesRef[count];
            final int perRegion = count / regions;
            for (int region = 0; region < regions; region++) {
                final int from = region * perRegion;
                final int to = region == regions - 1 ? count : from + perRegion;
                for (int i = from; i < to; i++) {
                    final int within = i - from;
                    final int term = (int) ((long) within * vocabulary.length / Math.max(1, to - from));
                    values[i] = bytes(vocabulary[Math.min(term, vocabulary.length - 1)]);
                }
            }
            return values;
        }
    },

    /** A trace id: entirely distinct, and long enough that the values dominate the column. */
    TRACE_ID {
        @Override
        BytesRef[] generate(int count, Random random) {
            final BytesRef[] values = new BytesRef[count];
            final StringBuilder builder = new StringBuilder(32);
            for (int i = 0; i < count; i++) {
                builder.setLength(0);
                builder.append(Long.toHexString(random.nextLong())).append(Long.toHexString(random.nextLong()));
                values[i] = bytes(builder.toString());
            }
            return values;
        }
    };

    abstract BytesRef[] generate(int count, Random random);

    /** Draws from {@code vocabulary} with a Zipf-like skew; {@code exponent} 0 draws uniformly. */
    private static BytesRef[] skewed(String[] vocabulary, int count, Random random, double exponent) {
        final BytesRef[] terms = new BytesRef[vocabulary.length];
        for (int i = 0; i < terms.length; i++) {
            terms[i] = bytes(vocabulary[i]);
        }
        final BytesRef[] values = new BytesRef[count];
        for (int i = 0; i < count; i++) {
            final int pick = exponent == 0.0
                ? random.nextInt(terms.length)
                : (int) (Math.pow(random.nextDouble(), exponent) * terms.length);
            values[i] = terms[Math.min(pick, terms.length - 1)];
        }
        return values;
    }

    private static BytesRef bytes(String value) {
        return new BytesRef(value.getBytes(StandardCharsets.UTF_8));
    }
}
