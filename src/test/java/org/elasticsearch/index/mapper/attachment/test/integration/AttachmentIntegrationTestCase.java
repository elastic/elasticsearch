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

package org.elasticsearch.index.mapper.attachment.test.integration;

import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matcher;
import org.junit.BeforeClass;

import static org.elasticsearch.plugin.mapper.attachments.tika.LocaleChecker.isLocaleCompatible;
import static org.hamcrest.Matchers.not;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class AttachmentIntegrationTestCase extends ElasticsearchIntegrationTest {

    protected static boolean expectError;

    @BeforeClass
    public static void expectErrorWithCurrentLocale() {
        expectError = !isLocaleCompatible();
    }

    protected static  <T> boolean assertThatWithError(T actual, Matcher<T> expected) {
        if (expectError) {
            assertThat(actual, not(expected));
        } else {
            assertThat(actual, expected);
        }
        return !expectError;
    }
}
