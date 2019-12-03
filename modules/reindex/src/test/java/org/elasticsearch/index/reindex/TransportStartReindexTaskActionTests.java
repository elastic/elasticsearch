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
package org.elasticsearch.index.reindex;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class TransportStartReindexTaskActionTests extends ESTestCase {

    public void testResolveIndexPatternOrder() {
        final AtomicLong time = new AtomicLong(randomLongBetween(0, 1L << 33));

        LongSupplier timeSupplier = () -> time.getAndAdd(randomLongBetween(0, 2));
        MetaData.Builder builder = MetaData.builder();
        int count = randomInt(9);
        IntStream.range(0, count).mapToObj(i -> indexMetaData("b-0" + i, timeSupplier.getAsLong())).forEach(builder::put);
        IntStream.range(0, count).mapToObj(i -> indexMetaData("c-0" + (9 - i), timeSupplier.getAsLong())).forEach(builder::put);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(builder).build();

        List<Set<String>> expectedCs = IntStream.range(0, count).mapToObj(i -> "c-0" + (9 - i))
            .sorted(Comparator.<String>comparingLong(n ->
                clusterState.metaData().getIndices().get(n).getCreationDate()).thenComparing(Function.identity()))
            .map(Set::of).collect(Collectors.toList());
        List<Set<String>> expectedBs = IntStream.range(0, count).mapToObj(i -> "b-0" + i)
            .map(Set::of).collect(Collectors.toList());

        List<Set<String>> expectedCB = Stream.concat(expectedCs.stream(), expectedBs.stream()).collect(Collectors.toList());
        List<Set<String>> expectedBC = Stream.concat(expectedBs.stream(), expectedCs.stream()).collect(Collectors.toList());

        checkResolved(clusterState, expectedCB, "c-*", "b-*");
        checkResolved(clusterState, expectedCB, "c-*", "*");
        checkResolved(clusterState, expectedBC, "*");
        checkResolved(clusterState, expectedBC, "*", "c-*");
        checkResolved(clusterState, expectedBC, "*", "c-*", "b-*");
        checkResolved(clusterState, expectedCs, "c-*");
        checkResolved(clusterState, expectedBs, "b-*");
    }

    public void testAliasOverlap() {
        MetaData.Builder builder = MetaData.builder();
        builder.put(indexMetaData("b", 0).putAlias(AliasMetaData.builder("a")).putAlias(AliasMetaData.builder("a2")));
        builder.put(indexMetaData("c", 0));
        builder.put(indexMetaData("d", 0).putAlias(AliasMetaData.builder("a2")));
        builder.put(indexMetaData("nothit", 0));
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(builder).build();

        checkResolved(clusterState, Arrays.asList(Set.of("b", "a"), Set.of("c")), "b", "c", "a");
        checkResolved(clusterState, Arrays.asList(Set.of("b", "a"), Set.of("c")), "a", "c", "b");
        checkResolved(clusterState, Arrays.asList(Set.of("b", "a"), Set.of("c")), "a", "b", "c");
        checkResolved(clusterState, Arrays.asList(Set.of("c"), Set.of("b", "a")), "c", "a", "b");
        checkResolved(clusterState, Arrays.asList(Set.of("a", "a2"), Set.of("c")), "a", "c", "a2");
        checkResolved(clusterState, Arrays.asList(Set.of("b", "a", "a2"), Set.of("c")), "b", "c", "a", "a2");
        checkResolved(clusterState, Arrays.asList(Set.of("b", "d", "a", "a2"), Set.of("c")), "b", "c", "d", "a", "a2");
        checkResolved(clusterState, Arrays.asList(Set.of("b", "d", "a", "a2"), Set.of("c")), "b", "c", "d", "a*");
    }

    public void testIndexOverlap() {
        MetaData.Builder builder = MetaData.builder();
        builder.put(indexMetaData("aa", 0));
        builder.put(indexMetaData("ab", 0));
        builder.put(indexMetaData("ab2", 0));
        builder.put(indexMetaData("nothit", 0));
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(builder).build();

        checkResolved(clusterState, Arrays.asList(Set.of("aa"), Set.of("ab"), Set.of("ab2")), "aa", "ab", "a*");
        checkResolved(clusterState, Arrays.asList(Set.of("ab"), Set.of("aa"), Set.of("ab2")), "ab", "a*");
        checkResolved(clusterState, Arrays.asList(Set.of("aa"), Set.of("ab"), Set.of("ab2")), "aa", "ab", "ab*");
        checkResolved(clusterState, Arrays.asList(Set.of("aa"), Set.of("ab"), Set.of("ab2")), "aa", "aa*", "ab*");
    }

    public void testRemoteReindex() {
        MetaData.Builder builder = MetaData.builder();
        builder.put(indexMetaData("a", 0));
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(builder).build();

        ReindexRequest request = new ReindexRequest().setSourceIndices("*");
        request.setRemoteInfo(new RemoteInfo("http", "localhost", -1, "/", new BytesArray("{}"), "me", "me", Collections.emptyMap(),
            TimeValue.ZERO, TimeValue.ZERO));
        List<Set<String>> resolved = TransportStartReindexTaskAction.resolveIndexPatterns(request,
            clusterState,
            new IndexNameExpressionResolver());

        assertEquals(Collections.emptyList(), resolved);
    }

    private IndexMetaData.Builder indexMetaData(String name, long creationDate) {
        return IndexMetaData.builder(name).settings(settings(Version.CURRENT))
            .creationDate(creationDate).numberOfShards(1).numberOfReplicas(0);
    }

    private void checkResolved(ClusterState clusterState, List<Set<String>> expected, String... input) {
        ReindexRequest request = new ReindexRequest().setSourceIndices(input);
        List<Set<String>> resolved = TransportStartReindexTaskAction.resolveIndexPatterns(request,
            clusterState,
            new IndexNameExpressionResolver());

        assertEquals(expected, resolved);
    }
}
