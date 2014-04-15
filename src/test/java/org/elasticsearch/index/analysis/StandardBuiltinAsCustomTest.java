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

package org.elasticsearch.index.analysis;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import java.io.IOException;

public class StandardBuiltinAsCustomTest extends AbstractBuiltinAsCustomTestBase {
    @Override
    protected BuiltinAsCustom buildBuiltinAsCustom() {
        return new StandardBuiltinAsCustom();
    }

    @Test
    public void keyword() throws IOException {
        check("keyword");
    }
    @Test
    public void simple() throws IOException {
        check("simple");
    }
    @Test
    public void whitespace() throws IOException {
        check("whitespace");
    }
    @Test
    public void stop() throws IOException {
        check("stop");
    }
    @Test
    public void snowball() throws IOException {
        check("snowball");
    }
    @Test
    public void standard() throws IOException {
        check("standard");
    }
    @Test
    public void arabic() throws IOException {
        check("arabic");
    }
    @Test
    public void armenian() throws IOException {
        check("armenian");
    }
    @Test
    public void basque() throws IOException {
        check("basque");
    }
    @Test
    public void brazillian() throws IOException {
        check("brazilian");
    }
    @Test
    public void bulgarian() throws IOException {
        check("bulgarian");
    }
    @Test
    public void catalan() throws IOException {
        check("catalan");
    }
    @Test(expected=IllegalArgumentException.class)
    public void chinese() throws IOException {
        new StandardBuiltinAsCustom().build(XContentBuilder.builder(JsonXContent.jsonXContent), "chinese");
    }
    @Test
    public void cjk() throws IOException {
        check("cjk");
    }
    @Test
    public void czech() throws IOException {
        check("czech");
    }
    @Test
    public void danish() throws IOException {
        check("danish");
    }
    @Test
    public void dutch() throws IOException {
        check("dutch");
    }
    @Test
    public void english() throws IOException {
        check("english");
    }
    @Test
    public void finnish() throws IOException {
        check("finnish");
    }
    @Test
    public void french() throws IOException {
        check("french");
    }
    @Test
    public void galician() throws IOException {
        check("galician");
    }
    @Test
    public void german() throws IOException {
        check("german");
    }
    @Test
    public void greek() throws IOException {
        check("greek");
    }
    @Test
    public void hindi() throws IOException {
        check("hindi");
    }
    @Test
    public void hungarian() throws IOException {
        check("hungarian");
    }
    @Test
    public void indonesian() throws IOException {
        check("indonesian");
    }
    @Test
    public void italian() throws IOException {
        check("italian");
    }
    @Test
    public void norwegian() throws IOException {
        check("norwegian");
    }
    @Test
    public void persian() throws IOException {
        check("persian");
    }
    @Test
    public void portuguese() throws IOException {
        check("portuguese");
    }
    @Test
    public void romanian() throws IOException {
        check("romanian");
    }
    @Test
    public void russian() throws IOException {
        check("russian");
    }
    @Test
    public void spanish() throws IOException {
        check("spanish");
    }
    @Test
    public void swedish() throws IOException {
        check("swedish");
    }
    @Test
    public void turkish() throws IOException {
        check("turkish");
    }
    @Test
    public void thai() throws IOException {
        check("thai");
    }
}
