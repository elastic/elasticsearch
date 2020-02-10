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

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class CoreValuesSourceTypeTests extends ESTestCase {

    public void testFromString() {
        assertThat(CoreValuesSourceType.fromString("numeric"), equalTo(CoreValuesSourceType.NUMERIC));
        assertThat(CoreValuesSourceType.fromString("bytes"), equalTo(CoreValuesSourceType.BYTES));
        assertThat(CoreValuesSourceType.fromString("geopoint"), equalTo(CoreValuesSourceType.GEOPOINT));
        assertThat(CoreValuesSourceType.fromString("range"), equalTo(CoreValuesSourceType.RANGE));
        assertThat(CoreValuesSourceType.fromString("histogram"), equalTo(CoreValuesSourceType.HISTOGRAM));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> CoreValuesSourceType.fromString("does_not_exist"));
        assertThat(e.getMessage(),
            equalTo("No enum constant org.elasticsearch.search.aggregations.support.CoreValuesSourceType.DOES_NOT_EXIST"));
        expectThrows(NullPointerException.class, () -> CoreValuesSourceType.fromString(null));
    }

    public void testSetValuesSourceTypeOnEmpty() {
        assertThat(CoreValuesSourceType.NUMERIC.getEmpty().getValuesSourceType(), equalTo(CoreValuesSourceType.NUMERIC));
        assertThat(CoreValuesSourceType.BYTES.getEmpty().getValuesSourceType(), equalTo(CoreValuesSourceType.BYTES));
        assertThat(CoreValuesSourceType.GEOPOINT.getEmpty().getValuesSourceType(), equalTo(CoreValuesSourceType.GEOPOINT));
        // RANGE doesn't currently have an empty case...
        //assertThat(CoreValuesSourceType.RANGE.getEmpty().getValuesSourceType(), equalTo(CoreValuesSourceType.RANGE));
        assertThat(CoreValuesSourceType.IP.getEmpty().getValuesSourceType(), equalTo(CoreValuesSourceType.IP));
        assertThat(CoreValuesSourceType.DATE.getEmpty().getValuesSourceType(), equalTo(CoreValuesSourceType.DATE));
        assertThat(CoreValuesSourceType.BOOLEAN.getEmpty().getValuesSourceType(), equalTo(CoreValuesSourceType.BOOLEAN));
    }

    public void testSetValuesSourceTypeOnScript() {
        assertThat(CoreValuesSourceType.NUMERIC.getScript(null, null).getValuesSourceType(), equalTo(CoreValuesSourceType.NUMERIC));
        assertThat(CoreValuesSourceType.BYTES.getScript(null, null).getValuesSourceType(), equalTo(CoreValuesSourceType.BYTES));
        // GEOPOINT, RANGE, and HISTOGRAM don't support scripts.
        //assertThat(CoreValuesSourceType.GEOPOINT.getScript(null, null).getValuesSourceType(), equalTo(CoreValuesSourceType.GEOPOINT));
        //assertThat(CoreValuesSourceType.RANGE.getScript(null, null).getValuesSourceType(), equalTo(CoreValuesSourceType.RANGE));
        assertThat(CoreValuesSourceType.IP.getScript(null, null).getValuesSourceType(), equalTo(CoreValuesSourceType.IP));
        assertThat(CoreValuesSourceType.DATE.getScript(null, null).getValuesSourceType(), equalTo(CoreValuesSourceType.DATE));
        assertThat(CoreValuesSourceType.BOOLEAN.getScript(null, null).getValuesSourceType(), equalTo(CoreValuesSourceType.BOOLEAN));
    }

    public void testSetValuesSourceTypeOnField() {
        FieldContext numeric = new FieldContext("number", mock(IndexNumericFieldData.class),
            new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE));
        assertThat(CoreValuesSourceType.NUMERIC.getField(numeric, null).getValuesSourceType(), equalTo(CoreValuesSourceType.NUMERIC));

        FieldContext bytes = new FieldContext("keyword", mock(IndexOrdinalsFieldData.class), new KeywordFieldMapper.KeywordFieldType());
        assertThat(CoreValuesSourceType.BYTES.getField(bytes, null).getValuesSourceType(), equalTo(CoreValuesSourceType.BYTES));

        FieldContext geo = new FieldContext("Geopoint", mock(IndexGeoPointFieldData.class), new GeoPointFieldMapper.GeoPointFieldType());
        assertThat(CoreValuesSourceType.GEOPOINT.getField(geo, null).getValuesSourceType(), equalTo(CoreValuesSourceType.GEOPOINT));

        FieldContext range = new FieldContext("Range", mock(IndexFieldData.class),
            new RangeFieldMapper.Builder("Range", RangeType.LONG).fieldType());
        assertThat(CoreValuesSourceType.RANGE.getField(range, null).getValuesSourceType(), equalTo(CoreValuesSourceType.RANGE));

        assertThat(CoreValuesSourceType.IP.getField(bytes, null).getValuesSourceType(), equalTo(CoreValuesSourceType.IP));
        assertThat(CoreValuesSourceType.DATE.getField(numeric, null).getValuesSourceType(), equalTo(CoreValuesSourceType.DATE));
        assertThat(CoreValuesSourceType.BOOLEAN.getField(numeric, null).getValuesSourceType(), equalTo(CoreValuesSourceType.BOOLEAN));

    }
}
