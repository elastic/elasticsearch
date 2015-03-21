/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.merge.policy;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.store.Store;

public abstract class AbstractMergePolicyProvider<MP extends MergePolicy> extends AbstractIndexShardComponent implements MergePolicyProvider<MP> {
    
    public static final String INDEX_COMPOUND_FORMAT = "index.compound_format";

    protected volatile double noCFSRatio;

    protected AbstractMergePolicyProvider(Store store) {
        super(store.shardId(), store.indexSettings());
        // Default to Lucene's default:
        this.noCFSRatio = parseNoCFSRatio(indexSettings.get(INDEX_COMPOUND_FORMAT, Double.toString(TieredMergePolicy.DEFAULT_NO_CFS_RATIO)));
    }

    public static double parseNoCFSRatio(String noCFSRatio) {
        noCFSRatio = noCFSRatio.trim();
        if (noCFSRatio.equalsIgnoreCase("true")) {
            return 1.0d;
        } else if (noCFSRatio.equalsIgnoreCase("false")) {
            return 0.0;
        } else {
            try {
                double value = Double.parseDouble(noCFSRatio);
                if (value < 0.0 || value > 1.0) {
                    throw new ElasticsearchIllegalArgumentException("NoCFSRatio must be in the interval [0..1] but was: [" + value + "]");
                }
                return value;
            } catch (NumberFormatException ex) {
                throw new ElasticsearchIllegalArgumentException("Expected a boolean or a value in the interval [0..1] but was: [" + noCFSRatio + "]", ex);
            }
        }
    }

    public static String formatNoCFSRatio(double ratio) {
        if (ratio == 1.0) {
            return Boolean.TRUE.toString();
        } else if (ratio == 0.0) {
            return Boolean.FALSE.toString();
        } else {
            return Double.toString(ratio);
        }
    }

}
