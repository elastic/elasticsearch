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

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.analysis.pattern;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.charfilter.MappingCharFilter;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.apache.lucene.analysis.path.PathHierarchyTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.Test;

public class XPatternTokenizerTests extends BaseTokenStreamTestCase
{
  @Test
  public void testSplitting() throws Exception 
  {
    String qpattern = "\\'([^\\']+)\\'"; // get stuff between "'"
    String[][] tests = {
      // group  pattern        input                    output
      { "-1",   "--",          "aaa--bbb--ccc",         "aaa bbb ccc" },
      { "-1",   ":",           "aaa:bbb:ccc",           "aaa bbb ccc" },
      { "-1",   "\\p{Space}",  "aaa   bbb \t\tccc  ",   "aaa bbb ccc" },
      { "-1",   ":",           "boo:and:foo",           "boo and foo" },
      { "-1",   "o",           "boo:and:foo",           "b :and:f" },
      { "0",    ":",           "boo:and:foo",           ": :" },
      { "0",    qpattern,      "aaa 'bbb' 'ccc'",       "'bbb' 'ccc'" },
      { "1",    qpattern,      "aaa 'bbb' 'ccc'",       "bbb ccc" }
    };
    
    for( String[] test : tests ) {     
      TokenStream stream = new XPatternTokenizer(newAttributeFactory(), Pattern.compile(test[1]), Integer.parseInt(test[0]));
      ((Tokenizer) stream).setReader(new StringReader(test[2]));
      String out = tsToString( stream );
      // System.out.println( test[2] + " ==> " + out );

      assertEquals("pattern: "+test[1]+" with input: "+test[2], test[3], out );
      
      // Make sure it is the same as if we called 'split'
      // test disabled, as we remove empty tokens
      /*if( "-1".equals( test[0] ) ) {
        String[] split = test[2].split( test[1] );
        stream = tokenizer.create( new StringReader( test[2] ) );
        int i=0;
        for( Token t = stream.next(); null != t; t = stream.next() ) 
        {
          assertEquals( "split: "+test[1] + " "+i, split[i++], new String(t.termBuffer(), 0, t.termLength()) );
        }
      }*/
    } 
  }

  @Test
  public void testOffsetCorrection() throws Exception {
    final String INPUT = "G&uuml;nther G&uuml;nther is here";

    // create MappingCharFilter
    List<String> mappingRules = new ArrayList<>();
    mappingRules.add( "\"&uuml;\" => \"ü\"" );
    NormalizeCharMap.Builder builder = new NormalizeCharMap.Builder();
    builder.add("&uuml;", "ü");
    NormalizeCharMap normMap = builder.build();
    CharFilter charStream = new MappingCharFilter( normMap, new StringReader( INPUT ) );

    // create PatternTokenizer
    Tokenizer stream = new XPatternTokenizer(newAttributeFactory(), Pattern.compile("[,;/\\s]+"), -1);
    stream.setReader(charStream);
    assertTokenStreamContents(stream,
        new String[] { "Günther", "Günther", "is", "here" },
        new int[] { 0, 13, 26, 29 },
        new int[] { 12, 25, 28, 33 },
        INPUT.length());
    
    charStream = new MappingCharFilter( normMap, new StringReader( INPUT ) );
    stream = new XPatternTokenizer(newAttributeFactory(), Pattern.compile("Günther"), 0);
    stream.setReader(charStream);
    assertTokenStreamContents(stream,
        new String[] { "Günther", "Günther" },
        new int[] { 0, 13 },
        new int[] { 12, 25 },
        INPUT.length());
  }
  
  /** 
   * TODO: rewrite tests not to use string comparison.
   */
  private static String tsToString(TokenStream in) throws IOException {
    StringBuilder out = new StringBuilder();
    CharTermAttribute termAtt = in.addAttribute(CharTermAttribute.class);
    // extra safety to enforce, that the state is not preserved and also
    // assign bogus values
    in.clearAttributes();
    termAtt.setEmpty().append("bogusTerm");
    in.reset();
    while (in.incrementToken()) {
      if (out.length() > 0)
        out.append(' ');
      out.append(termAtt.toString());
      in.clearAttributes();
      termAtt.setEmpty().append("bogusTerm");
    }

    in.close();
    return out.toString();
  }
  
  /** blast some random strings through the analyzer */
  @Test
  public void testRandomStrings() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new XPatternTokenizer(newAttributeFactory(), Pattern.compile("a"), -1);
        return new TokenStreamComponents(tokenizer);
      }    
    };
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
    
    Analyzer b = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new XPatternTokenizer(newAttributeFactory(), Pattern.compile("a"), 0);
        return new TokenStreamComponents(tokenizer);
      }    
    };
    checkRandomData(random(), b, 1000*RANDOM_MULTIPLIER);
  }
}
