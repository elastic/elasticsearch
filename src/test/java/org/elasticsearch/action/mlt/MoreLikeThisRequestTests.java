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

package org.elasticsearch.action.mlt;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.CoreMatchers.*;

public class MoreLikeThisRequestTests extends ElasticsearchTestCase {

    @Test
    public void testSerialization() throws IOException {

        MoreLikeThisRequest mltRequest = new MoreLikeThisRequest(randomAsciiOfLength(randomIntBetween(1, 20)))
                .id(randomAsciiOfLength(randomIntBetween(1, 20))).type(randomAsciiOfLength(randomIntBetween(1, 20)));

        if (randomBoolean()) {
            mltRequest.boostTerms(randomFloat());
        }
        if (randomBoolean()) {
            mltRequest.maxDocFreq(randomInt());
        }
        if (randomBoolean()) {
            mltRequest.minDocFreq(randomInt());
        }
        if (randomBoolean()) {
            mltRequest.maxQueryTerms(randomInt());
        }
        if (randomBoolean()) {
            mltRequest.minWordLength(randomInt());
        }
        if (randomBoolean()) {
            mltRequest.maxWordLength(randomInt());
        }
        if (randomBoolean()) {
            mltRequest.percentTermsToMatch(randomFloat());
        }
        if (randomBoolean()) {
            mltRequest.searchTypes(randomStrings(5));
        }
        if (randomBoolean()) {
            mltRequest.searchType(randomFrom(SearchType.values()));
        }
        if (randomBoolean()) {
            mltRequest.searchIndices(randomStrings(5));
        }
        if (randomBoolean()) {
            mltRequest.routing(randomAsciiOfLength(randomIntBetween(1, 20)));
        }
        if (randomBoolean()) {
            mltRequest.searchFrom(randomInt());
        }
        if (randomBoolean()) {
            mltRequest.searchSize(randomInt());
        }
        if (randomBoolean()) {
            mltRequest.searchScroll(new Scroll(TimeValue.timeValueNanos(randomLong())));
        }
        if (randomBoolean()) {
            mltRequest.searchSource(SearchSourceBuilder.searchSource().query(QueryBuilders.termQuery("term", "value")));
        }
        if(randomBoolean()) {
            mltRequest.include(randomBoolean());
        }
        if (randomBoolean()) {
            mltRequest.stopWords(randomStrings(10));
        }
        if (randomBoolean()) {
            mltRequest.fields(randomStrings(5));
        }

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(randomVersion(random()));
        mltRequest.writeTo(out);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        in.setVersion(out.getVersion());
        MoreLikeThisRequest mltRequest2 = new MoreLikeThisRequest();
        mltRequest2.readFrom(in);

        assertThat(mltRequest2.index(), equalTo(mltRequest.index()));
        assertThat(mltRequest2.type(), equalTo(mltRequest.type()));
        assertThat(mltRequest2.id(), equalTo(mltRequest.id()));
        assertThat(mltRequest2.boostTerms(), equalTo(mltRequest.boostTerms()));
        assertThat(mltRequest2.maxDocFreq(), equalTo(mltRequest.maxDocFreq()));
        assertThat(mltRequest2.minDocFreq(), equalTo(mltRequest.minDocFreq()));
        assertThat(mltRequest2.maxQueryTerms(), equalTo(mltRequest.maxQueryTerms()));
        assertThat(mltRequest2.minWordLength(), equalTo(mltRequest.minWordLength()));
        assertThat(mltRequest2.maxWordLength(), equalTo(mltRequest.maxWordLength()));
        assertThat(mltRequest2.percentTermsToMatch(), equalTo(mltRequest.percentTermsToMatch()));
        assertThat(mltRequest2.searchTypes(), equalTo(mltRequest.searchTypes()));
        assertThat(mltRequest2.searchType(), equalTo(mltRequest.searchType()));
        assertThat(mltRequest2.searchIndices(), equalTo(mltRequest.searchIndices()));
        assertThat(mltRequest2.routing(), equalTo(mltRequest.routing()));
        assertThat(mltRequest2.searchFrom(), equalTo(mltRequest.searchFrom()));
        assertThat(mltRequest2.searchSize(), equalTo(mltRequest.searchSize()));
        if (mltRequest.searchScroll() == null) {
            assertThat(mltRequest2.searchScroll(), nullValue());
        } else {
            assertThat(mltRequest2.searchFrom(), notNullValue());
            assertThat(mltRequest2.searchScroll().keepAlive(), equalTo(mltRequest.searchScroll().keepAlive()));
        }

        if (mltRequest.searchSource() == null) {
            assertThat(mltRequest2.searchSource().length(), equalTo(0));
        } else {
            assertThat(mltRequest2.searchSource().length(), equalTo(mltRequest.searchSource().length()));
        }

        if (mltRequest.stopWords() != null && mltRequest.stopWords().length > 0) {
            assertThat(mltRequest2.stopWords(), equalTo(mltRequest.stopWords()));
        } else {
            assertThat(mltRequest2.stopWords(), nullValue());
        }
        if (mltRequest.fields() == null) {
            assertThat(mltRequest2.fields(), equalTo(Strings.EMPTY_ARRAY));
        } else {
            assertThat(mltRequest2.fields(), equalTo(mltRequest.fields()));
        }
        assertThat(mltRequest2.include(), equalTo(mltRequest.include()));
    }

    private static String[] randomStrings(int max) {
        int count = randomIntBetween(0, max);
        String[] strings = new String[count];
        for (int i = 0; i < strings.length; i++) {
            strings[i] = randomAsciiOfLength(randomIntBetween(1, 20));
        }
        return strings;
    }
}
