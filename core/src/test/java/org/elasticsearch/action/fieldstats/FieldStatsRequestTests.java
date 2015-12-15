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

package org.elasticsearch.action.fieldstats;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.StreamsUtils;

import static org.elasticsearch.action.fieldstats.IndexConstraint.Comparison.*;
import static org.elasticsearch.action.fieldstats.IndexConstraint.Property.MAX;
import static org.elasticsearch.action.fieldstats.IndexConstraint.Property.MIN;
import static org.hamcrest.Matchers.equalTo;

public class FieldStatsRequestTests extends ESTestCase {

    public void testFieldsParsing() throws Exception {
        byte[] data = StreamsUtils.copyToBytesFromClasspath("/org/elasticsearch/action/fieldstats/fieldstats-index-constraints-request.json");
        FieldStatsRequest request = new FieldStatsRequest();
        request.source(new BytesArray(data));

        assertThat(request.getFields().length, equalTo(5));
        assertThat(request.getFields()[0], equalTo("field1"));
        assertThat(request.getFields()[1], equalTo("field2"));
        assertThat(request.getFields()[2], equalTo("field3"));
        assertThat(request.getFields()[3], equalTo("field4"));
        assertThat(request.getFields()[4], equalTo("field5"));

        assertThat(request.getIndexConstraints().length, equalTo(8));
        assertThat(request.getIndexConstraints()[0].getField(), equalTo("field2"));
        assertThat(request.getIndexConstraints()[0].getValue(), equalTo("9"));
        assertThat(request.getIndexConstraints()[0].getProperty(), equalTo(MAX));
        assertThat(request.getIndexConstraints()[0].getComparison(), equalTo(GTE));
        assertThat(request.getIndexConstraints()[1].getField(), equalTo("field3"));
        assertThat(request.getIndexConstraints()[1].getValue(), equalTo("5"));
        assertThat(request.getIndexConstraints()[1].getProperty(), equalTo(MIN));
        assertThat(request.getIndexConstraints()[1].getComparison(), equalTo(GT));
        assertThat(request.getIndexConstraints()[2].getField(), equalTo("field4"));
        assertThat(request.getIndexConstraints()[2].getValue(), equalTo("a"));
        assertThat(request.getIndexConstraints()[2].getProperty(), equalTo(MIN));
        assertThat(request.getIndexConstraints()[2].getComparison(), equalTo(GTE));
        assertThat(request.getIndexConstraints()[3].getField(), equalTo("field4"));
        assertThat(request.getIndexConstraints()[3].getValue(), equalTo("g"));
        assertThat(request.getIndexConstraints()[3].getProperty(), equalTo(MAX));
        assertThat(request.getIndexConstraints()[3].getComparison(), equalTo(LTE));
        assertThat(request.getIndexConstraints()[4].getField(), equalTo("field5"));
        assertThat(request.getIndexConstraints()[4].getValue(), equalTo("2"));
        assertThat(request.getIndexConstraints()[4].getProperty(), equalTo(MAX));
        assertThat(request.getIndexConstraints()[4].getComparison(), equalTo(GT));
        assertThat(request.getIndexConstraints()[5].getField(), equalTo("field5"));
        assertThat(request.getIndexConstraints()[5].getValue(), equalTo("9"));
        assertThat(request.getIndexConstraints()[5].getProperty(), equalTo(MAX));
        assertThat(request.getIndexConstraints()[5].getComparison(), equalTo(LT));
        assertThat(request.getIndexConstraints()[6].getField(), equalTo("field1"));
        assertThat(request.getIndexConstraints()[6].getValue(), equalTo("2014-01-01"));
        assertThat(request.getIndexConstraints()[6].getProperty(), equalTo(MIN));
        assertThat(request.getIndexConstraints()[6].getComparison(), equalTo(GTE));
        assertThat(request.getIndexConstraints()[6].getOptionalFormat(), equalTo("date_optional_time"));
        assertThat(request.getIndexConstraints()[7].getField(), equalTo("field1"));
        assertThat(request.getIndexConstraints()[7].getValue(), equalTo("2015-01-01"));
        assertThat(request.getIndexConstraints()[7].getProperty(), equalTo(MAX));
        assertThat(request.getIndexConstraints()[7].getComparison(), equalTo(LT));
        assertThat(request.getIndexConstraints()[7].getOptionalFormat(), equalTo("date_optional_time"));
    }

}
