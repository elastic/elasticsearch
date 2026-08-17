/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.util.List;
import java.util.Locale;

/**
 * One publicly-published corpus: a logical dataset with a 1:1 workload csv-spec (for
 * {@link Kind#WORKLOAD}) and one or more physical {@link VariantSpec variants}. Adding a corpus
 * costs exactly two files and no Java: a {@code corpora:} entry plus a
 * {@code public-<corpus>.csv-spec}.
 *
 * @param id          unique kebab/snake-case corpus id; also the dataset name queried by
 *                    {@code FROM <id>} in the workload
 * @param title       human-readable name
 * @param registryUrl where the corpus is published/documented
 * @param license     upstream license of the data
 * @param description what the corpus is and which matrix cells it adds
 * @param kind        {@link Kind#WORKLOAD} (has a csv-spec) or {@link Kind#FAILURE_ONLY} (only
 *                    {@code expect_failure:} variants; the dirty-data carve-out)
 * @param scale       corpus-level scale bucket
 * @param quality     corpus-level data quality
 * @param workload    the csv-spec resource name, e.g. {@code public-clickbench.csv-spec}; null for
 *                    failure-only corpora
 * @param variants    the physical incarnations
 */
public record CorpusSpec(
    String id,
    String title,
    String registryUrl,
    String license,
    String description,
    Kind kind,
    Scale scale,
    DataQuality quality,
    String workload,
    List<VariantSpec> variants
) {

    public enum Kind {
        WORKLOAD,
        FAILURE_ONLY;

        public static Kind fromId(String id) {
            return valueOf(id.toUpperCase(Locale.ROOT).replace('-', '_'));
        }
    }

    /** Variants that run in the active suite (active provider, not backup entries). */
    public List<VariantSpec> activeVariants() {
        return variants.stream().filter(VariantSpec::active).toList();
    }
}
