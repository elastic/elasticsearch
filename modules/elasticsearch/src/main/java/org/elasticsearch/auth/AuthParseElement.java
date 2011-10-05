/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.auth;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 * @author @timetabling
 */
public class AuthParseElement implements SearchParseElement {

    protected final ESLogger logger = Loggers.getLogger(getClass());

    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token;
        String login = null;
        String pw = null;
        int depth = 0;
        String name = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                name = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("login".equals(name))
                    login = parser.text();
                else if ("password".equals(name))
                    pw = parser.text();
                else
                    throw new SearchParseException(context, "Authentication not correctly specified. Use login and password [" + name + "]");
            } else if (token == XContentParser.Token.START_OBJECT) {
                depth++;
            }

            // do not allow nested
            if (depth != 1)
                throw new SearchParseException(context, "Authentication structure not correct. Use login and password only [" + depth + "]");
        }

        logger.info("login:{} pw:{}", login, pw);
        context.auth(new SearchContextAuth(login, pw));
    }
}
