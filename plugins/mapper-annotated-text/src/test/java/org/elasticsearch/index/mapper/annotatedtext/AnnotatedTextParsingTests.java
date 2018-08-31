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

package org.elasticsearch.index.mapper.annotatedtext;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotatedText;
import org.elasticsearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotatedText.AnnotationToken;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class AnnotatedTextParsingTests extends ESTestCase {
    
    private void checkParsing(String markup, String expectedPlainText, AnnotationToken... expectedTokens) {
        AnnotatedText at = AnnotatedText.parse(markup);
        assertEquals(expectedPlainText, at.textMinusMarkup);
        List<AnnotationToken> actualAnnotations = at.annotations;
        assertEquals(expectedTokens.length, actualAnnotations.size());
        for (int i = 0; i < expectedTokens.length; i++) {
            assertEquals(expectedTokens[i], actualAnnotations.get(i));
        }
    }  
    
    public void testSingleValueMarkup() {
        checkParsing("foo [bar](Y)", "foo bar", new AnnotationToken(4,7,"Y"));
    }   
    
    public void testMultiValueMarkup() {
        checkParsing("foo [bar](Y&B)", "foo bar", new AnnotationToken(4,7,"Y"), 
                new AnnotationToken(4,7,"B"));
    }    
    
    public void testBlankTextAnnotation() {
        checkParsing("It sounded like this:[](theSoundOfOneHandClapping)", "It sounded like this:", 
                new AnnotationToken(21,21,"theSoundOfOneHandClapping"));
    }    
    
    public void testMissingBracket() {
        checkParsing("[foo](MissingEndBracket bar",
                "[foo](MissingEndBracket bar", new AnnotationToken[0]);
    }
    
    public void testAnnotationWithType() {
        Exception expectedException = expectThrows(ElasticsearchParseException.class,
                () -> checkParsing("foo [bar](type=foo) baz", "foo bar baz",  new AnnotationToken(4,7, "noType")));
            assertThat(expectedException.getMessage(), equalTo("key=value pairs are not supported in annotations"));
    }
    
    public void testMissingValue() {
        checkParsing("[foo]() bar", "foo bar", new AnnotationToken[0]);
    }    
        

}
