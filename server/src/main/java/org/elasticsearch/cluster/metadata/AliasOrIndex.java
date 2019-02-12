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

package org.elasticsearch.cluster.metadata;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Encapsulates the  {@link IndexMetaData} instances of a concrete index or indices an alias is pointing to.
 */
public interface AliasOrIndex {

    /**
     * @return whether this an alias or concrete index
     */
    boolean isAlias();

    /**
     * @return All {@link IndexMetaData} of all concrete indices this alias is referring to
     * or if this is a concrete index its {@link IndexMetaData}
     */
    List<IndexMetaData> getIndices();

    /**
     * Represents an concrete index and encapsulates its {@link IndexMetaData}
     */
    class Index implements AliasOrIndex {

        private final IndexMetaData concreteIndex;

        public Index(IndexMetaData indexMetaData) {
            this.concreteIndex = indexMetaData;
        }

        @Override
        public boolean isAlias() {
            return false;
        }

        @Override
        public List<IndexMetaData> getIndices() {
            return Collections.singletonList(concreteIndex);
        }
    }

    /**
     * Represents an alias and groups all {@link IndexMetaData} instances sharing the same alias name together.
     */
    class Alias implements AliasOrIndex {

        private final String aliasName;
        private final List<IndexMetaData> referenceIndexMetaDatas;
        private final SetOnce<IndexMetaData> writeIndex = new SetOnce<>();

        public Alias(AliasMetaData aliasMetaData, IndexMetaData indexMetaData) {
            this.aliasName = aliasMetaData.getAlias();
            this.referenceIndexMetaDatas = new ArrayList<>();
            this.referenceIndexMetaDatas.add(indexMetaData);
        }

        @Override
        public boolean isAlias() {
            return true;
        }

        public String getAliasName() {
            return aliasName;
        }

        @Override
        public List<IndexMetaData> getIndices() {
            return referenceIndexMetaDatas;
        }


        @Nullable
        public IndexMetaData getWriteIndex() {
            return writeIndex.get();
        }

        /**
         * Returns the unique alias metadata per concrete index.
         *
         * (note that although alias can point to the same concrete indices, each alias reference may have its own routing
         * and filters)
         */
        public Iterable<Tuple<String, AliasMetaData>> getConcreteIndexAndAliasMetaDatas() {
            return () -> new Iterator<Tuple<String,AliasMetaData>>() {

                int index = 0;

                @Override
                public boolean hasNext() {
                    return index < referenceIndexMetaDatas.size();
                }

                @Override
                public Tuple<String, AliasMetaData> next() {
                    IndexMetaData indexMetaData = referenceIndexMetaDatas.get(index++);
                    return new Tuple<>(indexMetaData.getIndex().getName(), indexMetaData.getAliases().get(aliasName));
                }
            };
        }

        public AliasMetaData getFirstAliasMetaData() {
            return referenceIndexMetaDatas.get(0).getAliases().get(aliasName);
        }

        void addIndex(IndexMetaData indexMetaData) {
            this.referenceIndexMetaDatas.add(indexMetaData);
        }

        public void computeAndValidateWriteIndex() {
            List<IndexMetaData> writeIndices = referenceIndexMetaDatas.stream()
                .filter(idxMeta -> Boolean.TRUE.equals(idxMeta.getAliases().get(aliasName).writeIndex()))
                .collect(Collectors.toList());

            if (writeIndices.isEmpty() && referenceIndexMetaDatas.size() == 1
                    && referenceIndexMetaDatas.get(0).getAliases().get(aliasName).writeIndex() == null) {
                writeIndices.add(referenceIndexMetaDatas.get(0));
            }

            if (writeIndices.size() == 1) {
                writeIndex.set(writeIndices.get(0));
            } else if (writeIndices.size() > 1) {
                List<String> writeIndicesStrings = writeIndices.stream()
                    .map(i -> i.getIndex().getName()).collect(Collectors.toList());
                throw new IllegalStateException("alias [" + aliasName + "] has more than one write index [" +
                    Strings.collectionToCommaDelimitedString(writeIndicesStrings) + "]");
            }
        }
    }
}
