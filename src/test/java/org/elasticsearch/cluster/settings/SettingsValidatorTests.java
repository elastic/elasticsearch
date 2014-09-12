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

package org.elasticsearch.cluster.settings;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class SettingsValidatorTests extends ElasticsearchTestCase {

    @Test
    public void testValidators() throws Exception {
        assertThat(Validator.EMPTY.validate("", "anything goes"), nullValue());

        assertThat(Validator.TIME.validate("", "10m"), nullValue());
        assertThat(Validator.TIME.validate("", "10g"), notNullValue());
        assertThat(Validator.TIME.validate("", "bad timing"), notNullValue());

        assertThat(Validator.BYTES_SIZE.validate("", "10m"), nullValue());
        assertThat(Validator.BYTES_SIZE.validate("", "10g"), nullValue());
        assertThat(Validator.BYTES_SIZE.validate("", "bad"), notNullValue());

        assertThat(Validator.FLOAT.validate("", "10.2"), nullValue());
        assertThat(Validator.FLOAT.validate("", "10.2.3"), notNullValue());

        assertThat(Validator.NON_NEGATIVE_FLOAT.validate("", "10.2"), nullValue());
        assertThat(Validator.NON_NEGATIVE_FLOAT.validate("", "0.0"), nullValue());
        assertThat(Validator.NON_NEGATIVE_FLOAT.validate("", "-1.0"), notNullValue());
        assertThat(Validator.NON_NEGATIVE_FLOAT.validate("", "10.2.3"), notNullValue());

        assertThat(Validator.DOUBLE.validate("", "10.2"), nullValue());
        assertThat(Validator.DOUBLE.validate("", "10.2.3"), notNullValue());

        assertThat(Validator.DOUBLE_GTE_2.validate("", "10.2"), nullValue());
        assertThat(Validator.DOUBLE_GTE_2.validate("", "2.0"), nullValue());
        assertThat(Validator.DOUBLE_GTE_2.validate("", "1.0"), notNullValue());
        assertThat(Validator.DOUBLE_GTE_2.validate("", "10.2.3"), notNullValue());

        assertThat(Validator.NON_NEGATIVE_DOUBLE.validate("", "10.2"), nullValue());
        assertThat(Validator.NON_NEGATIVE_DOUBLE.validate("", "0.0"), nullValue());
        assertThat(Validator.NON_NEGATIVE_DOUBLE.validate("", "-1.0"), notNullValue());
        assertThat(Validator.NON_NEGATIVE_DOUBLE.validate("", "10.2.3"), notNullValue());

        assertThat(Validator.INTEGER.validate("", "10"), nullValue());
        assertThat(Validator.INTEGER.validate("", "10.2"), notNullValue());

        assertThat(Validator.INTEGER_GTE_2.validate("", "2"), nullValue());
        assertThat(Validator.INTEGER_GTE_2.validate("", "1"), notNullValue());
        assertThat(Validator.INTEGER_GTE_2.validate("", "0"), notNullValue());
        assertThat(Validator.INTEGER_GTE_2.validate("", "10.2.3"), notNullValue());

        assertThat(Validator.NON_NEGATIVE_INTEGER.validate("", "2"), nullValue());
        assertThat(Validator.NON_NEGATIVE_INTEGER.validate("", "1"), nullValue());
        assertThat(Validator.NON_NEGATIVE_INTEGER.validate("", "0"), nullValue());
        assertThat(Validator.NON_NEGATIVE_INTEGER.validate("", "-1"), notNullValue());
        assertThat(Validator.NON_NEGATIVE_INTEGER.validate("", "10.2"), notNullValue());

        assertThat(Validator.POSITIVE_INTEGER.validate("", "2"), nullValue());
        assertThat(Validator.POSITIVE_INTEGER.validate("", "1"), nullValue());
        assertThat(Validator.POSITIVE_INTEGER.validate("", "0"), notNullValue());
        assertThat(Validator.POSITIVE_INTEGER.validate("", "-1"), notNullValue());
        assertThat(Validator.POSITIVE_INTEGER.validate("", "10.2"), notNullValue());
    }

    @Test
    public void testDynamicValidators() throws Exception {
        DynamicSettings ds = new DynamicSettings();
        ds.addDynamicSetting("my.test.*", Validator.POSITIVE_INTEGER);
        String valid = ds.validateDynamicSetting("my.test.setting", "-1");
        assertThat(valid, equalTo("the value of the setting my.test.setting must be a positive integer"));
    }
}
