/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.util.Nullable;
import org.elasticsearch.util.concurrent.ThreadSafe;

/**
 * @author kimchy (Shay Banon)
 */
@ThreadSafe
public interface DocumentMapper {

    String type();

    /**
     * When constructed by parsing a mapping definition, will return it. Otherwise,
     * returns <tt>null</tt>.
     */
    String mappingSource();

    /**
     * Generates the source of the mapper based on the current mappings.
     */
    String buildSource() throws FailedToGenerateSourceMapperException;

    /**
     * Merges this document mapper with the provided document mapper.
     */
    void merge(DocumentMapper mergeWith, MergeFlags mergeFlags) throws MergeMappingException;

    UidFieldMapper uidMapper();

    IdFieldMapper idMapper();

    TypeFieldMapper typeMapper();

    SourceFieldMapper sourceMapper();

    BoostFieldMapper boostMapper();

    DocumentFieldMappers mappers();

    /**
     * The default index analyzer to be used. Note, the {@link DocumentFieldMappers#indexAnalyzer()} should
     * probably be used instead.
     */
    Analyzer indexAnalyzer();

    /**
     * The default search analyzer to be used. Note, the {@link DocumentFieldMappers#searchAnalyzer()} should
     * probably be used instead.
     */
    Analyzer searchAnalyzer();

    /**
     * Parses the source into a parsed document.
     * <p/>
     * <p>Validates that the source has the provided id and type. Note, most times
     * we will already have the id and the type even though they exist in the source as well.
     */
    ParsedDocument parse(@Nullable String type, @Nullable String id, byte[] source) throws MapperParsingException;

    /**
     * Parses the source into the parsed document.
     */
    ParsedDocument parse(byte[] source) throws MapperParsingException;

    /**
     * Adds a field mapper listener.
     */
    void addFieldMapperListener(FieldMapperListener fieldMapperListener, boolean includeExisting);

    public static class MergeFlags {

        public static MergeFlags mergeFlags() {
            return new MergeFlags();
        }

        private boolean simulate = true;

        private boolean ignoreDuplicates = false;

        public MergeFlags() {
        }

        public boolean simulate() {
            return simulate;
        }

        public MergeFlags simulate(boolean simulate) {
            this.simulate = simulate;
            return this;
        }

        public boolean ignoreDuplicates() {
            return ignoreDuplicates;
        }

        public MergeFlags ignoreDuplicates(boolean ignoreDuplicates) {
            this.ignoreDuplicates = ignoreDuplicates;
            return this;
        }
    }
}
