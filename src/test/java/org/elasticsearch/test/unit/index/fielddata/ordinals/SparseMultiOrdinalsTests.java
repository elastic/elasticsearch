/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.elasticsearch.test.unit.index.fielddata.ordinals;

import org.apache.lucene.util.IntsRef;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.fielddata.ordinals.SparseMultiArrayOrdinals;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.testng.Assert.fail;

/**
 */
public class SparseMultiOrdinalsTests extends MultiOrdinalsTests {

    @Override
    protected Ordinals creationMultiOrdinals(OrdinalsBuilder builder, ImmutableSettings.Builder settings) {
        settings.put("multi_ordinals", "sparse");
        return builder.build(settings.build());
    }

    @Test
    public void testMultiValuesSurpassOrdinalsLimit() throws Exception {
        OrdinalsBuilder builder = new OrdinalsBuilder(2);
        int maxOrds = 128;
        for (int i = 0; i < maxOrds; i++) {
            builder.nextOrdinal();
            if (i == 2 || i == 4) {
                builder.addDoc(0);
            }
            builder.addDoc(1);
            
        }
        
        try {
            Builder builder2 = ImmutableSettings.builder();
            builder2.put("multi_ordinals_max_docs", 64);
            creationMultiOrdinals(builder, builder2);
            fail("Exception should have been throwed");
        } catch (ElasticSearchException e) {

        }
    }

    @Test
    public void testMultiValuesDocsWithOverlappingStorageArrays() throws Exception {
        int maxDoc = 7;
        int maxOrds = 15;
        OrdinalsBuilder builder = new OrdinalsBuilder(maxDoc);
        for (int i = 0; i < maxOrds; i++) {
            builder.nextOrdinal();
            if (i < 10) {
                builder.addDoc(0);
            }
            builder.addDoc(1);
            if (i == 0) {
                builder.addDoc(2);
            }
            if (i < 5) {
                builder.addDoc(3);

            }
            if (i < 6) {
                builder.addDoc(4);

            }
            if (i == 1) {
                builder.addDoc(5);
            }
            if (i < 10) {
                builder.addDoc(6);
            }
        }
      
        Ordinals ordinals = new SparseMultiArrayOrdinals(builder, 64);
        Ordinals.Docs docs = ordinals.ordinals();
        assertThat(docs.getNumDocs(), equalTo(maxDoc));
        assertThat(docs.getNumOrds(), equalTo(maxOrds)); // Includes null ord
        assertThat(docs.isMultiValued(), equalTo(true));

        // Document 1
        assertThat(docs.getOrd(0), equalTo(1));
        IntsRef ref = docs.getOrds(0);
        assertThat(ref.offset, equalTo(0));
        for (int i = 0; i < 10; i++) {
            assertThat(ref.ints[i], equalTo(i + 1));
        }
        assertThat(ref.length, equalTo(10));

        // Document 2
        assertThat(docs.getOrd(1), equalTo(1));
        ref = docs.getOrds(1);
        assertThat(ref.offset, equalTo(0));
        for (int i = 0; i < 15; i++) {
            assertThat(ref.ints[i], equalTo(i + 1));
        }
        assertThat(ref.length, equalTo(15));

        // Document 3
        assertThat(docs.getOrd(2), equalTo(1));
        ref = docs.getOrds(2);
        assertThat(ref.offset, equalTo(0));
        assertThat(ref.ints[0], equalTo(1));
        assertThat(ref.length, equalTo(1));

        // Document 4
        assertThat(docs.getOrd(3), equalTo(1));
        ref = docs.getOrds(3);
        assertThat(ref.offset, equalTo(0));
        for (int i = 0; i < 5; i++) {
            assertThat(ref.ints[i], equalTo(i + 1));
        }
        assertThat(ref.length, equalTo(5));

        // Document 5
        assertThat(docs.getOrd(4), equalTo(1));
        ref = docs.getOrds(4);
        assertThat(ref.offset, equalTo(0));
        for (int i = 0; i < 6; i++) {
            assertThat(ref.ints[i], equalTo(i + 1));
        }
        assertThat(ref.length, equalTo(6));

        // Document 6
        assertThat(docs.getOrd(5), equalTo(2));
        ref = docs.getOrds(5);
        assertThat(ref.offset, equalTo(0));
        assertThat(ref.ints[0], equalTo(2));
        assertThat(ref.length, equalTo(1));

        // Document 7
        assertThat(docs.getOrd(6), equalTo(1));
        ref = docs.getOrds(6);
        assertThat(ref.offset, equalTo(0));
        for (int i = 0; i < 10; i++) {
            assertThat(ref.ints[i], equalTo(i + 1));
        }
        assertThat(ref.length, equalTo(10));
    }

}
