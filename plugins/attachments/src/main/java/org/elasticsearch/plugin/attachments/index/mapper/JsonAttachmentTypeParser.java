/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.plugin.attachments.index.mapper;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.json.JsonDateFieldMapper;
import org.elasticsearch.index.mapper.json.JsonMapper;
import org.elasticsearch.index.mapper.json.JsonStringFieldMapper;
import org.elasticsearch.index.mapper.json.JsonTypeParser;

import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.index.mapper.json.JsonTypeParsers.*;

/**
 * <pre>
 *  field1 : { type : "attachment" }
 * </pre>
 * Or:
 * <pre>
 *  field1 : {
 *      type : "attachment",
 *      fields : {
 *          field1 : {type : "binary"},
 *          title : {store : "yes"},
 *          date : {store : "yes"}
 *      }
 * }
 * </pre>
 *
 * @author kimchy (shay.banon)
 */
public class JsonAttachmentTypeParser implements JsonTypeParser {

    @Override public JsonMapper.Builder parse(String name, JsonNode node, ParserContext parserContext) throws MapperParsingException {
        ObjectNode attachmentNode = (ObjectNode) node;
        JsonAttachmentMapper.Builder builder = new JsonAttachmentMapper.Builder(name);

        for (Iterator<Map.Entry<String, JsonNode>> fieldsIt = attachmentNode.getFields(); fieldsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = fieldsIt.next();
            String fieldName = entry.getKey();
            JsonNode fieldNode = entry.getValue();
            if (fieldName.equals("pathType")) {
                builder.pathType(parsePathType(name, fieldNode.getValueAsText()));
            } else if (fieldName.equals("fields")) {
                ObjectNode fieldsNode = (ObjectNode) fieldNode;
                for (Iterator<Map.Entry<String, JsonNode>> propsIt = fieldsNode.getFields(); propsIt.hasNext();) {
                    Map.Entry<String, JsonNode> entry1 = propsIt.next();
                    String propName = entry1.getKey();
                    JsonNode propNode = entry1.getValue();

                    if (name.equals(propName)) {
                        // that is the content
                        builder.content((JsonStringFieldMapper.Builder) parserContext.typeParser("string").parse(name, propNode, parserContext));
                    } else if ("date".equals(propName)) {
                        builder.date((JsonDateFieldMapper.Builder) parserContext.typeParser("date").parse("date", propNode, parserContext));
                    } else if ("title".equals(propName)) {
                        builder.title((JsonStringFieldMapper.Builder) parserContext.typeParser("string").parse("title", propNode, parserContext));
                    } else if ("author".equals(propName)) {
                        builder.author((JsonStringFieldMapper.Builder) parserContext.typeParser("string").parse("author", propNode, parserContext));
                    } else if ("keywords".equals(propName)) {
                        builder.keywords((JsonStringFieldMapper.Builder) parserContext.typeParser("string").parse("keywords", propNode, parserContext));
                    }
                }
            }
        }


        return builder;
    }
}
