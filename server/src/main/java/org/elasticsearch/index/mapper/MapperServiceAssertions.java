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

package org.elasticsearch.index.mapper;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.Assertions;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.compress.CompressedXContent;

import java.util.Map;

/**
 * This class exists so that we can disable these assertions in a mixed-version cluster without disabling all assertions in
 * {@link MapperService}. These assertions can not necessarily hold in a mixed-version cluster because older nodes will not be serializing
 * mapping version.
 */
// TODO: this indirection can be removed when all nodes in a mixed-version cluster test understand mapping version
final class MapperServiceAssertions {

    private MapperServiceAssertions() {

    }

    /**
     * Assertions regarding changes in the mapping version.
     *
     * @param currentIndexMetaData the current index metadata
     * @param newIndexMetaData the new index metadata
     * @param updatedEntries the updated document mappers
     */
    static void assertMappingVersion(
            final IndexMetaData currentIndexMetaData,
            final IndexMetaData newIndexMetaData,
            final Map<String, DocumentMapper> updatedEntries) {
        if (Assertions.ENABLED && currentIndexMetaData != null) {
            if (currentIndexMetaData.getMappingVersion() == newIndexMetaData.getMappingVersion()) {
                // if the mapping version is unchanged, then there should not be any updates and all mappings should be the same
                assert updatedEntries.isEmpty() : updatedEntries;
                for (final ObjectCursor<MappingMetaData> mapping : newIndexMetaData.getMappings().values()) {
                    final CompressedXContent currentSource = currentIndexMetaData.mapping(mapping.value.type()).source();
                    final CompressedXContent newSource = mapping.value.source();
                    assert currentSource.equals(newSource) :
                            "expected current mapping [" + currentSource + "] for type [" + mapping.value.type() + "] "
                                    + "to be the same as new mapping [" + newSource + "]";
                }
            } else {
                // if the mapping version is changed, it should increase, there should be updates, and the mapping should be different
                final long currentMappingVersion = currentIndexMetaData.getMappingVersion();
                final long newMappingVersion = newIndexMetaData.getMappingVersion();
                assert currentMappingVersion < newMappingVersion :
                        "expected current mapping version [" + currentMappingVersion + "] "
                                + "to be less than new mapping version [" + newMappingVersion + "]";
                assert updatedEntries.isEmpty() == false;
                for (final DocumentMapper documentMapper : updatedEntries.values()) {
                    final MappingMetaData currentMapping = currentIndexMetaData.mapping(documentMapper.type());
                    if (currentMapping != null) {
                        final CompressedXContent currentSource = currentMapping.source();
                        final CompressedXContent newSource = documentMapper.mappingSource();
                        assert currentSource.equals(newSource) == false :
                                "expected current mapping [" + currentSource + "] for type [" + documentMapper.type() + "] " +
                                        "to be different than new mapping";
                    }
                }
            }
        }
    }

}
