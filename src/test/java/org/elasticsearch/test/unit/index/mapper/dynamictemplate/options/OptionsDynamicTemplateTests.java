/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.unit.index.mapper.dynamictemplate.options;

import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.ByteArrayOutputStream;

import org.elasticsearch.test.unit.index.mapper.dynamictemplate.pathmatch.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.FieldInfo;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.field.data.strings.StringFieldDataType;
import org.elasticsearch.index.field.data.longs.LongFieldDataType;

import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.ObjectMappers;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.elasticsearch.index.mapper.core.*;
import org.elasticsearch.index.mapper.object.ObjectMapper;
//import org.elasticsearch.index.mapper.*;
//import static org.elasticsearch.index.mapper.MapperBuilders.*;


import org.elasticsearch.test.unit.index.mapper.MapperTests;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import org.elasticsearch.common.xcontent.ToXContent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 *
 */
public class OptionsDynamicTemplateTests {

    @Test
    public void testSimpleOptions() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/test/unit/index/mapper/dynamictemplate/options/test-mapping.json");
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/test/unit/index/mapper/dynamictemplate/options/test-data.json");
        Document doc = docMapper.parse(new BytesArray(json)).rootDoc();

        // Print the mapping
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        XContentBuilder builder = XContentFactory.jsonBuilder(bos);        
        builder.prettyPrint();
        builder.startObject();
        docMapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.flush();
  //     System.out.printf("map:\n%s\n", bos.toString("UTF8"));
     
        // Validate the updated mapping
        ObjectMapper detailsMapper = docMapper.objectMappers().get("details__o");
        assertThat(detailsMapper.fullPath(), equalTo("details__o"));
        
        FieldMappers fieldMappers = docMapper.mappers().fullName("details__o.age__l");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
        assertThat(fieldMappers.mapper().names().indexName(), equalTo("details.age"));
        assertThat(fieldMappers.mapper().fieldDataType(), instanceOf(LongFieldDataType.class));
        
        fieldMappers = docMapper.mappers().fullName("details__o.gender__si");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
        assertThat(fieldMappers.mapper().names().indexName(), equalTo("details.gender"));
        assertThat(fieldMappers.mapper().fieldDataType(), instanceOf(StringFieldDataType.class));
        assertThat(fieldMappers.mapper().analyzed(), equalTo(false));
        assertThat(fieldMappers.mapper().omitNorms(), equalTo(true));
        assertThat(fieldMappers.mapper().indexOptions(),  equalTo(IndexOptions.DOCS_ONLY));
        
        fieldMappers = docMapper.mappers().fullName("details__o.name_first__sni");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
        assertThat(fieldMappers.mapper().names().indexName(), equalTo("details.name"));
        assertThat(fieldMappers.mapper().fieldDataType(), instanceOf(StringFieldDataType.class));
        assertThat(fieldMappers.mapper().analyzed(), equalTo(false));
        assertThat(fieldMappers.mapper().omitNorms(), equalTo(true));
        assertThat(fieldMappers.mapper().indexOptions(),  equalTo(IndexOptions.DOCS_ONLY));
        
        fieldMappers = docMapper.mappers().fullName("details__o.name_initial__sj");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
        assertThat(fieldMappers.mapper().fieldDataType(), instanceOf(StringFieldDataType.class));
        assertThat(fieldMappers.mapper().indexed(), equalTo(false));
        
        fieldMappers = docMapper.mappers().fullName("details__o.name_last__sni");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
        assertThat(fieldMappers.mapper().names().indexName(), equalTo("details.name"));
        assertThat(fieldMappers.mapper().fieldDataType(), instanceOf(StringFieldDataType.class));
        assertThat(fieldMappers.mapper().analyzed(), equalTo(false));
        assertThat(fieldMappers.mapper().omitNorms(), equalTo(true));
        assertThat(fieldMappers.mapper().indexOptions(),  equalTo(IndexOptions.DOCS_ONLY));

        fieldMappers = docMapper.mappers().fullName("details__o.position__s");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
        assertThat(fieldMappers.mapper().names().indexName(), equalTo("details.position"));
        assertThat(fieldMappers.mapper().fieldDataType(), instanceOf(StringFieldDataType.class));
        
        fieldMappers = docMapper.mappers().fullName("details__o.status__si");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
        assertThat(fieldMappers.mapper().names().indexName(), equalTo("details.status"));
        assertThat(fieldMappers.mapper().fieldDataType(), instanceOf(StringFieldDataType.class));
        assertThat(fieldMappers.mapper().analyzed(), equalTo(false));
        assertThat(fieldMappers.mapper().omitNorms(), equalTo(true));
        assertThat(fieldMappers.mapper().indexOptions(),  equalTo(IndexOptions.DOCS_ONLY));
        
        fieldMappers = docMapper.mappers().fullName("details__o.dob__d");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
        assertThat(fieldMappers.mapper().names().indexName(), equalTo("details.dob"));
        assertThat(fieldMappers.mapper().fieldDataType(), instanceOf(LongFieldDataType.class));

        
        ObjectMapper contactMapper = docMapper.objectMappers().get("contact__o");
        assertThat(contactMapper.fullPath(), equalTo("contact__o"));

        ObjectMapper addressMapper = docMapper.objectMappers().get("contact__o.address");
        assertThat(addressMapper.fullPath(), equalTo("contact__o.address"));

        fieldMappers = docMapper.mappers().fullName("contact__o.address.city__si");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
        assertThat(fieldMappers.mapper().names().indexName(), equalTo("contact.address.city"));
        assertThat(fieldMappers.mapper().fieldDataType(), instanceOf(StringFieldDataType.class));
        assertThat(fieldMappers.mapper().analyzed(), equalTo(false));
        assertThat(fieldMappers.mapper().omitNorms(), equalTo(true));
        assertThat(fieldMappers.mapper().indexOptions(),  equalTo(IndexOptions.DOCS_ONLY));
        
        fieldMappers = docMapper.mappers().fullName("contact__o.address.lines__s");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
        assertThat(fieldMappers.mapper().names().indexName(), equalTo("contact.address.lines"));
        assertThat(fieldMappers.mapper().fieldDataType(), instanceOf(StringFieldDataType.class));
        assertThat(fieldMappers.mapper().analyzed(), equalTo(true));

        fieldMappers = docMapper.mappers().fullName("contact__o.address.pcode__si");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
        assertThat(fieldMappers.mapper().names().indexName(), equalTo("contact.address.pcode"));
        assertThat(fieldMappers.mapper().fieldDataType(), instanceOf(StringFieldDataType.class));
        assertThat(fieldMappers.mapper().analyzed(), equalTo(false));
        assertThat(fieldMappers.mapper().omitNorms(), equalTo(true));
        assertThat(fieldMappers.mapper().indexOptions(),  equalTo(IndexOptions.DOCS_ONLY));
        
        fieldMappers = docMapper.mappers().fullName("contact__o.address.state__si");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
        assertThat(fieldMappers.mapper().names().indexName(), equalTo("contact.address.state"));
        assertThat(fieldMappers.mapper().fieldDataType(), instanceOf(StringFieldDataType.class));
        assertThat(fieldMappers.mapper().analyzed(), equalTo(false));
        assertThat(fieldMappers.mapper().omitNorms(), equalTo(true));
        assertThat(fieldMappers.mapper().indexOptions(),  equalTo(IndexOptions.DOCS_ONLY));
        
        
        ObjectMapper inetMapper = docMapper.objectMappers().get("contact__o.inet");
        assertThat(inetMapper.fullPath(), equalTo("contact__o.inet"));

        fieldMappers = docMapper.mappers().fullName("contact__o.inet.address__s");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
        assertThat(fieldMappers.mapper().names().indexName(), equalTo("contact.inet.address"));
        assertThat(fieldMappers.mapper().fieldDataType(), instanceOf(StringFieldDataType.class));
        assertThat(fieldMappers.mapper().analyzed(), equalTo(true));
        assertThat(fieldMappers.mapper().omitNorms(), equalTo(false));
        assertThat(fieldMappers.mapper().indexOptions(),  equalTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS));
        
        fieldMappers = docMapper.mappers().fullName("contact__o.inet.type__si");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
        assertThat(fieldMappers.mapper().names().indexName(), equalTo("contact.inet.type"));
        assertThat(fieldMappers.mapper().fieldDataType(), instanceOf(StringFieldDataType.class));
        assertThat(fieldMappers.mapper().analyzed(), equalTo(false));
        assertThat(fieldMappers.mapper().omitNorms(), equalTo(true));
        assertThat(fieldMappers.mapper().indexOptions(),  equalTo(IndexOptions.DOCS_ONLY));

        
        ObjectMapper phoneMapper = docMapper.objectMappers().get("contact__o.phone");
        assertThat(phoneMapper.fullPath(), equalTo("contact__o.phone"));

        fieldMappers = docMapper.mappers().fullName("contact__o.phone.acode__si");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
        assertThat(fieldMappers.mapper().names().indexName(), equalTo("contact.phone.acode"));
        assertThat(fieldMappers.mapper().fieldDataType(), instanceOf(StringFieldDataType.class));
        assertThat(fieldMappers.mapper().analyzed(), equalTo(false));
        assertThat(fieldMappers.mapper().indexed(), equalTo(true));
        
        fieldMappers = docMapper.mappers().fullName("contact__o.phone.number__si");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
        assertThat(fieldMappers.mapper().names().indexName(), equalTo("contact.phone.number"));
        assertThat(fieldMappers.mapper().fieldDataType(), instanceOf(StringFieldDataType.class));
        assertThat(fieldMappers.mapper().analyzed(), equalTo(false));
        
        ObjectMapper changeMapper = docMapper.objectMappers().get("change");
        assertThat(changeMapper.fullPath(), equalTo("change"));
        assertThat(changeMapper.dynamic(), equalTo(ObjectMapper.Dynamic.TRUE));
        
        ObjectMapper verMapper = docMapper.objectMappers().get("change.versions__ode");
        assertThat(verMapper.fullPath(), equalTo("change.versions__ode"));
        assertThat(verMapper.dynamic(), equalTo(ObjectMapper.Dynamic.FALSE));
        
        
//        System.out.printf("doc:\n%s\n", doc.toString());
        
    }
}
