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

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.core.*;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.internal.*;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.index.mapper.ip.EphemeralIpFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.mapper.object.RootObjectMapper;

/**
 *
 */
public final class MapperBuilders {

    private MapperBuilders() {

    }

    public static DocumentMapper.Builder doc(String index, Settings settings, RootObjectMapper.Builder objectBuilder) {
        return new DocumentMapper.Builder(index, settings, objectBuilder);
    }

    public static SourceFieldMapper.Builder source() {
        return new SourceFieldMapper.Builder();
    }

    public static IdFieldMapper.Builder id() {
        return new IdFieldMapper.Builder();
    }

    public static RoutingFieldMapper.Builder routing() {
        return new RoutingFieldMapper.Builder();
    }

    public static UidFieldMapper.Builder uid() {
        return new UidFieldMapper.Builder();
    }

    public static SizeFieldMapper.Builder size() {
        return new SizeFieldMapper.Builder();
    }

    public static VersionFieldMapper.Builder version() {
        return new VersionFieldMapper.Builder();
    }

    public static TypeFieldMapper.Builder type() {
        return new TypeFieldMapper.Builder();
    }

    public static FieldNamesFieldMapper.Builder fieldNames() {
        return new FieldNamesFieldMapper.Builder();
    }

    public static IndexFieldMapper.Builder index() {
        return new IndexFieldMapper.Builder();
    }

    public static TimestampFieldMapper.Builder timestamp() {
        return new TimestampFieldMapper.Builder();
    }

    public static TTLFieldMapper.Builder ttl() {
        return new TTLFieldMapper.Builder();
    }

    public static ParentFieldMapper.Builder parent() {
        return new ParentFieldMapper.Builder();
    }

    public static BoostFieldMapper.Builder boost(String name) {
        return new BoostFieldMapper.Builder(name);
    }

    public static AllFieldMapper.Builder all() {
        return new AllFieldMapper.Builder();
    }

    public static AnalyzerMapper.Builder analyzer() {
        return new AnalyzerMapper.Builder();
    }

    public static RootObjectMapper.Builder rootObject(String name) {
        return new RootObjectMapper.Builder(name);
    }

    public static ObjectMapper.Builder object(String name) {
        return new ObjectMapper.Builder(name);
    }

    public static BooleanFieldMapper.Builder booleanField(String name) {
        return new BooleanFieldMapper.Builder(name);
    }

    public static StringFieldMapper.Builder stringField(String name) {
        return new StringFieldMapper.Builder(name);
    }

    public static BinaryFieldMapper.Builder binaryField(String name) {
        return new BinaryFieldMapper.Builder(name);
    }

    public static DateFieldMapper.Builder dateField(String name) {
        return new DateFieldMapper.Builder(name);
    }

    public static IpFieldMapper.Builder ipField(String name) {
        return new IpFieldMapper.Builder(name);
    }

    public static ShortFieldMapper.Builder shortField(String name) {
        return new ShortFieldMapper.Builder(name);
    }

    public static ByteFieldMapper.Builder byteField(String name) {
        return new ByteFieldMapper.Builder(name);
    }

    public static IntegerFieldMapper.Builder integerField(String name) {
        return new IntegerFieldMapper.Builder(name);
    }

    public static TokenCountFieldMapper.Builder tokenCountField(String name) {
        return new TokenCountFieldMapper.Builder(name);
    }

    public static LongFieldMapper.Builder longField(String name) {
        return new LongFieldMapper.Builder(name);
    }

    public static Murmur3FieldMapper.Builder murmur3Field(String name) {
        return new Murmur3FieldMapper.Builder(name);
    }

    public static FloatFieldMapper.Builder floatField(String name) {
        return new FloatFieldMapper.Builder(name);
    }

    public static DoubleFieldMapper.Builder doubleField(String name) {
        return new DoubleFieldMapper.Builder(name);
    }

    public static GeoPointFieldMapper.Builder geoPointField(String name) {
        return new GeoPointFieldMapper.Builder(name);
    }

    public static GeoShapeFieldMapper.Builder geoShapeField(String name) {
        return new GeoShapeFieldMapper.Builder(name);
    }

    public static CompletionFieldMapper.Builder completionField(String name) {
        return new CompletionFieldMapper.Builder(name);
    }

    // Added by Loggly - START
    //
    
    public static EphemeralByteFieldMapper.Builder ephemeralByteField(String name) {
        return new EphemeralByteFieldMapper.Builder(name);
    }
    public static EphemeralShortFieldMapper.Builder ephemeralShortField(String name) {
        return new EphemeralShortFieldMapper.Builder(name);
    }
    public static EphemeralIntegerFieldMapper.Builder ephemeralIntegerField(String name) {
        return new EphemeralIntegerFieldMapper.Builder(name);
    }
    public static EphemeralLongFieldMapper.Builder ephemeralLongField(String name) {
        return new EphemeralLongFieldMapper.Builder(name);
    }
    public static EphemeralFloatFieldMapper.Builder ephemeralFloatField(String name) {
        return new EphemeralFloatFieldMapper.Builder(name);
    }
    public static EphemeralDoubleFieldMapper.Builder ephemeralDoubleField(String name) {
        return new EphemeralDoubleFieldMapper.Builder(name);
    }
    public static EphemeralBooleanFieldMapper.Builder ephemeralBooleanField(String name) {
        return new EphemeralBooleanFieldMapper.Builder(name);
    }
    public static EphemeralDateFieldMapper.Builder ephemeralDateField(String name) {
        return new EphemeralDateFieldMapper.Builder(name);
    }
    public static EphemeralIpFieldMapper.Builder ephemeralIpField(String name) {
        return new EphemeralIpFieldMapper.Builder(name);
    }
    public static EphemeralStringFieldMapper.Builder ephemeralStringField(String name) {
        return new EphemeralStringFieldMapper.Builder(name);
    }

    public static Mapper.Builder getEphemeralBuilder(String typeName, String fieldName) {
        if (EphemeralByteFieldMapper.CONTENT_TYPE.equals(typeName)) {
            return new EphemeralByteFieldMapper.Builder(fieldName);
        }
        if (EphemeralShortFieldMapper.CONTENT_TYPE.equals(typeName)) {
            return new EphemeralShortFieldMapper.Builder(fieldName);
        }
        if (EphemeralIntegerFieldMapper.CONTENT_TYPE.equals(typeName)) {
            return new EphemeralIntegerFieldMapper.Builder(fieldName);
        }
        if (EphemeralLongFieldMapper.CONTENT_TYPE.equals(typeName)) {
            return new EphemeralLongFieldMapper.Builder(fieldName);
        }
        if (EphemeralFloatFieldMapper.CONTENT_TYPE.equals(typeName)) {
            return new EphemeralFloatFieldMapper.Builder(fieldName);
        }
        if (EphemeralDoubleFieldMapper.CONTENT_TYPE.equals(typeName)) {
            return new EphemeralDoubleFieldMapper.Builder(fieldName);
        }
        if (EphemeralBooleanFieldMapper.CONTENT_TYPE.equals(typeName)) {
            return new EphemeralBooleanFieldMapper.Builder(fieldName);
        }
        if (EphemeralDateFieldMapper.CONTENT_TYPE.equals(typeName)) {
            return new EphemeralDateFieldMapper.Builder(fieldName);
        }
        if (EphemeralIpFieldMapper.CONTENT_TYPE.equals(typeName)) {
            return new EphemeralIpFieldMapper.Builder(fieldName);
        }
        if (EphemeralStringFieldMapper.CONTENT_TYPE.equals(typeName)) {
            return new EphemeralStringFieldMapper.Builder(fieldName);
        }
        
        return null;
    }
    //
    // Added by Loggly - END

}
