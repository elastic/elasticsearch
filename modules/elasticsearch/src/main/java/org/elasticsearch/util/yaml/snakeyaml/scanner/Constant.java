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

package org.elasticsearch.util.yaml.snakeyaml.scanner;

public final class Constant {
    private final static String LINEBR_S = "\n\u0085\u2028\u2029";
    private final static String FULL_LINEBR_S = "\r" + LINEBR_S;
    private final static String NULL_OR_LINEBR_S = "\0" + FULL_LINEBR_S;
    private final static String NULL_BL_LINEBR_S = " " + NULL_OR_LINEBR_S;
    private final static String NULL_BL_T_LINEBR_S = "\t" + NULL_BL_LINEBR_S;

    public final static Constant LINEBR = new Constant(LINEBR_S);
    public final static Constant FULL_LINEBR = new Constant(FULL_LINEBR_S);
    public final static Constant NULL_OR_LINEBR = new Constant(NULL_OR_LINEBR_S);
    public final static Constant NULL_BL_LINEBR = new Constant(NULL_BL_LINEBR_S);
    public final static Constant NULL_BL_T_LINEBR = new Constant(NULL_BL_T_LINEBR_S);

    private String content;

    private Constant(String content) {
        this.content = content;
    }

    public boolean has(char ch) {
        return content.indexOf(ch) != -1;
    }

    public boolean hasNo(char ch) {
        return !has(ch);
    }

    public boolean has(char ch, String additional) {
        return additional.indexOf(ch) != -1 || content.indexOf(ch) != -1;
    }

    public boolean hasNo(char ch, String additional) {
        return !has(ch, additional);
    }
}
