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

package org.elasticsearch.client.core;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

//similar to SearchRequestTests as CountRequest inline several members (and functionality) from SearchRequest
public class CountRequestTests extends ESTestCase {

    public void testIllegalArguments() {
        CountRequest countRequest = new CountRequest();
        assertNotNull(countRequest.indices());
        assertNotNull(countRequest.indicesOptions());
        assertNotNull(countRequest.types());

        NullPointerException e = expectThrows(NullPointerException.class, () -> countRequest.indices((String[]) null));
        assertEquals("indices must not be null", e.getMessage());
        e = expectThrows(NullPointerException.class, () -> countRequest.indices((String) null));
        assertEquals("index must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> countRequest.indicesOptions(null));
        assertEquals("indicesOptions must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> countRequest.types((String[]) null));
        assertEquals("types must not be null", e.getMessage());
        e = expectThrows(NullPointerException.class, () -> countRequest.types((String) null));
        assertEquals("type must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> countRequest.source(null));
        assertEquals("source must not be null", e.getMessage());

    }

    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(createCountRequest(), CountRequestTests::copyRequest, this::mutate);
    }

    private CountRequest createCountRequest() {
        CountRequest countRequest = new CountRequest("index");
        countRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("num", 10)));
        return countRequest;
    }

    private CountRequest mutate(CountRequest countRequest) {
        CountRequest mutation = copyRequest(countRequest);
        List<Runnable> mutators = new ArrayList<>();
        mutators.add(() -> mutation.indices(ArrayUtils.concat(countRequest.indices(), new String[]{randomAlphaOfLength(10)})));
        mutators.add(() -> mutation.indicesOptions(randomValueOtherThan(countRequest.indicesOptions(),
            () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()))));
        mutators.add(() -> mutation.types(ArrayUtils.concat(countRequest.types(), new String[]{randomAlphaOfLength(10)})));
        mutators.add(() -> mutation.preference(randomValueOtherThan(countRequest.preference(), () -> randomAlphaOfLengthBetween(3, 10))));
        mutators.add(() -> mutation.routing(randomValueOtherThan(countRequest.routing(), () -> randomAlphaOfLengthBetween(3, 10))));
        randomFrom(mutators).run();
        return mutation;
    }

    private static CountRequest copyRequest(CountRequest countRequest) {
        CountRequest result = new CountRequest();
        result.indices(countRequest.indices());
        result.indicesOptions(countRequest.indicesOptions());
        result.types(countRequest.types());
        result.routing(countRequest.routing());
        result.preference(countRequest.preference());
        if (countRequest.source() != null) {
            result.source(countRequest.source());
        }
        return result;
    }
}
