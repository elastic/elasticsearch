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

package org.elasticsearch.action.admin.indices.mapping.put;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.common.collect.Tuple.tuple;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;

public class PutMappingRequestTests extends ESTestCase {

    public void testValidation() {
        PutMappingRequest r = new PutMappingRequest("myindex");
        ActionRequestValidationException ex = r.validate();
        assertNotNull("source validation should fail", ex);
        assertTrue(ex.getMessage().contains("source is missing"));

        r.source("", XContentType.JSON);
        ex = r.validate();
        assertNotNull("source validation should fail", ex);
        assertTrue(ex.getMessage().contains("source is empty"));

        r.source("somevalidmapping", XContentType.JSON);
        ex = r.validate();
        assertNull("validation should succeed", ex);

        r.setConcreteIndex(new Index("foo", "bar"));
        ex = r.validate();
        assertNotNull("source validation should fail", ex);
        assertEquals(ex.getMessage(),
            "Validation Failed: 1: either concrete index or unresolved indices can be set," +
                " concrete index: [[foo/bar]] and indices: [myindex];");
    }

    /**
     * Test that {@link PutMappingRequest#simpleMapping(String...)}
     * rejects inputs where the {@code Object...} varargs of field name and properties are not
     * paired correctly
     */
    public void testBuildFromSimplifiedDef() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> PutMappingRequest.simpleMapping("only_field"));
        assertEquals("mapping source must be pairs of fieldnames and properties definition.", e.getMessage());
    }

    public void testResolveIndicesWithWriteIndexOnlyAndDataStreamsAndWriteAliases() {
        String[] dataStreamNames = {"foo", "bar", "baz"};
        List<Tuple<String, Integer>> dsMetadata = List.of(
            tuple(dataStreamNames[0], randomIntBetween(1, 3)),
            tuple(dataStreamNames[1], randomIntBetween(1, 3)),
            tuple(dataStreamNames[2], randomIntBetween(1, 3)));

        ClusterState cs = DataStreamTestHelper.getClusterStateWithDataStreams(dsMetadata, List.of("index1", "index2", "index3"));
        cs = addAliases(cs, List.of(
            tuple("alias1", List.of(tuple("index1", false), tuple("index2", true))),
            tuple("alias2", List.of(tuple("index2", false), tuple("index3", true)))
        ));
        PutMappingRequest request = new PutMappingRequest().indices("foo", "alias1", "alias2").writeIndexOnly(true);
        Index[] indices = TransportPutMappingAction.resolveIndices(cs, request, new IndexNameExpressionResolver());
        List<String> indexNames = Arrays.stream(indices).map(Index::getName).collect(Collectors.toList());
        IndexAbstraction expectedDs = cs.metadata().getIndicesLookup().get("foo");
        // should resolve the data stream and each alias to their respective write indices
        assertThat(indexNames, containsInAnyOrder(expectedDs.getWriteIndex().getIndex().getName(), "index2", "index3"));
    }

    public void testResolveIndicesWithoutWriteIndexOnlyAndDataStreamsAndWriteAliases() {
        String[] dataStreamNames = {"foo", "bar", "baz"};
        List<Tuple<String, Integer>> dsMetadata = List.of(
            tuple(dataStreamNames[0], randomIntBetween(1, 3)),
            tuple(dataStreamNames[1], randomIntBetween(1, 3)),
            tuple(dataStreamNames[2], randomIntBetween(1, 3)));

        ClusterState cs = DataStreamTestHelper.getClusterStateWithDataStreams(dsMetadata, List.of("index1", "index2", "index3"));
        cs = addAliases(cs, List.of(
            tuple("alias1", List.of(tuple("index1", false), tuple("index2", true))),
            tuple("alias2", List.of(tuple("index2", false), tuple("index3", true)))
        ));
        PutMappingRequest request = new PutMappingRequest().indices("foo", "alias1", "alias2");
        Index[] indices = TransportPutMappingAction.resolveIndices(cs, request, new IndexNameExpressionResolver());
        List<String> indexNames = Arrays.stream(indices).map(Index::getName).collect(Collectors.toList());
        IndexAbstraction expectedDs = cs.metadata().getIndicesLookup().get("foo");
        List<String> expectedIndices = expectedDs.getIndices().stream().map(im -> im.getIndex().getName()).collect(Collectors.toList());
        expectedIndices.addAll(List.of("index1", "index2", "index3"));
        // should resolve the data stream and each alias to _all_ their respective indices
        assertThat(indexNames, containsInAnyOrder(expectedIndices.toArray()));
    }

    public void testResolveIndicesWithWriteIndexOnlyAndDataStreamAndIndex() {
        String[] dataStreamNames = {"foo", "bar", "baz"};
        List<Tuple<String, Integer>> dsMetadata = List.of(
            tuple(dataStreamNames[0], randomIntBetween(1, 3)),
            tuple(dataStreamNames[1], randomIntBetween(1, 3)),
            tuple(dataStreamNames[2], randomIntBetween(1, 3)));

        ClusterState cs = DataStreamTestHelper.getClusterStateWithDataStreams(dsMetadata, List.of("index1", "index2", "index3"));
        cs = addAliases(cs, List.of(
            tuple("alias1", List.of(tuple("index1", false), tuple("index2", true))),
            tuple("alias2", List.of(tuple("index2", false), tuple("index3", true)))
        ));
        PutMappingRequest request = new PutMappingRequest().indices("foo", "index3").writeIndexOnly(true);
        Index[] indices = TransportPutMappingAction.resolveIndices(cs, request, new IndexNameExpressionResolver());
        List<String> indexNames = Arrays.stream(indices).map(Index::getName).collect(Collectors.toList());
        IndexAbstraction expectedDs = cs.metadata().getIndicesLookup().get("foo");
        List<String> expectedIndices = expectedDs.getIndices().stream().map(im -> im.getIndex().getName()).collect(Collectors.toList());
        expectedIndices.addAll(List.of("index1", "index2", "index3"));
        // should resolve the data stream and each alias to _all_ their respective indices
        assertThat(indexNames, containsInAnyOrder(expectedDs.getWriteIndex().getIndex().getName(), "index3"));
    }

    public void testResolveIndicesWithWriteIndexOnlyAndNoSingleWriteIndex() {
        String[] dataStreamNames = {"foo", "bar", "baz"};
        List<Tuple<String, Integer>> dsMetadata = List.of(
            tuple(dataStreamNames[0], randomIntBetween(1, 3)),
            tuple(dataStreamNames[1], randomIntBetween(1, 3)),
            tuple(dataStreamNames[2], randomIntBetween(1, 3)));

        ClusterState cs = DataStreamTestHelper.getClusterStateWithDataStreams(dsMetadata, List.of("index1", "index2", "index3"));
        final ClusterState cs2 = addAliases(cs, List.of(
            tuple("alias1", List.of(tuple("index1", false), tuple("index2", true))),
            tuple("alias2", List.of(tuple("index2", false), tuple("index3", true)))
        ));
        PutMappingRequest request = new PutMappingRequest().indices("*").writeIndexOnly(true);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> TransportPutMappingAction.resolveIndices(cs2, request, new IndexNameExpressionResolver()));
        assertThat(e.getMessage(), containsString("The index expression [*] and options provided did not point to a single write-index"));
    }

    public void testResolveIndicesWithWriteIndexOnlyAndAliasWithoutWriteIndex() {
        String[] dataStreamNames = {"foo", "bar", "baz"};
        List<Tuple<String, Integer>> dsMetadata = List.of(
            tuple(dataStreamNames[0], randomIntBetween(1, 3)),
            tuple(dataStreamNames[1], randomIntBetween(1, 3)),
            tuple(dataStreamNames[2], randomIntBetween(1, 3)));

        ClusterState cs = DataStreamTestHelper.getClusterStateWithDataStreams(dsMetadata, List.of("index1", "index2", "index3"));
        final ClusterState cs2 = addAliases(cs, List.of(
            tuple("alias1", List.of(tuple("index1", false), tuple("index2", false))),
            tuple("alias2", List.of(tuple("index2", false), tuple("index3", false)))
        ));
        PutMappingRequest request = new PutMappingRequest().indices("alias2").writeIndexOnly(true);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> TransportPutMappingAction.resolveIndices(cs2, request, new IndexNameExpressionResolver()));
        assertThat(e.getMessage(), containsString("no write index is defined for alias [alias2]"));
    }

    /**
     * Adds aliases to the supplied ClusterState instance. The aliases parameter takes of list of tuples of aliasName
     * to the alias's indices. The alias's indices are a tuple of index name and a flag indicating whether the alias
     * is a write alias for that index. See usage examples above.
     */
    private static ClusterState addAliases(ClusterState cs, List<Tuple<String, List<Tuple<String, Boolean>>>> aliases) {
        Metadata.Builder builder = Metadata.builder(cs.metadata());
        for (Tuple<String, List<Tuple<String, Boolean>>> alias : aliases) {
            for (Tuple<String, Boolean> index : alias.v2()) {
                IndexMetadata im = builder.get(index.v1());
                AliasMetadata newAliasMd = AliasMetadata.newAliasMetadataBuilder(alias.v1()).writeIndex(index.v2()).build();
                builder.put(IndexMetadata.builder(im).putAlias(newAliasMd));
            }
        }
        return ClusterState.builder(cs).metadata(builder.build()).build();
    }

}
