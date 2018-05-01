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

package org.elasticsearch;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.Strings;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.InvalidNameException;
import org.elasticsearch.indices.Naming;

import static org.hamcrest.Matchers.endsWith;

public class NamingTests extends LuceneTestCase {

    public void testInvalidCharsFails() {
        validateIndexName("index?name", "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
    }

    public void testContainsHashFails() {
        validateIndexName("index#name", "must not contain '#'");
    }

    public void testStartUnderscoreFails() {
        validateIndexName("_indexname", "must not start with '_', '-', or '+'");
    }

    public void testStartMinusFails() {
        validateIndexName("-indexname", "must not start with '_', '-', or '+'");
    }

    public void testStartPlusFails() {
        validateIndexName("+indexname", "must not start with '_', '-', or '+'");
    }

    public void testUppercaseFails() {
        validateIndexName("INDEXNAME", "must be lowercase");
    }

    public void testDoubleDotFails() {
        validateIndexName("..", "must not be '.' or '..'");
    }

    public void testDotFails() {
        validateIndexName(".", "must not be '.' or '..'");
    }

    public void testContainsColonFails() {
        validateIndexName("foo:bar", "must not contain ':'");
    }

    public void testAliasNameInvalidCharsFails() {
        validate(Naming.ALIAS,
            "alias?name",
            "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS,
            InvalidAliasNameException.class);
    }

    private void validateIndexName(String indexName, String errorMessage) {
        validate(Naming.INDEX, indexName, errorMessage, InvalidIndexNameException.class);
    }

    private <T extends InvalidNameException> void validate(Naming naming, String indexName, String errorMessage, final Class<T> thrown) {
        T e = expectThrows(thrown, () -> naming.validate(indexName));
        assertThat(e.getMessage(), endsWith(errorMessage));
    }
}
