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

package org.elasticsearch.search;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.Serializable;

/**
 * Class for holding fined-grained details of a parse error including
 * line and column numbers with a message. Can be serialized using
 * regular Java serialization (for use between nodes) and can supply 
 * details as XContent (typically for REST responses). 
 */
public class ParseErrorDetails implements ToXContent, Serializable {
    
    private String parseMsg;
    private int parseErrorLine = -1;
    private int parseErrorCol = -1;
    
    public ParseErrorDetails(String parseMsg, int parseErrorLine, int parseErrorCol) {
        super();
        this.parseMsg = parseMsg;
        this.parseErrorLine = parseErrorLine;
        this.parseErrorCol = parseErrorCol;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("parse_failure");
        builder.field("line", parseErrorLine);
        builder.field("col", parseErrorCol);
        builder.field("message", parseMsg);
        builder.endObject();                
        return builder;
    }
    
}
