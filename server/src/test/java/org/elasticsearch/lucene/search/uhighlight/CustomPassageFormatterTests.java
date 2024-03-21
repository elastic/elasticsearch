/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.search.uhighlight;

import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.SimpleHTMLEncoder;
import org.apache.lucene.search.uhighlight.Passage;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

public class CustomPassageFormatterTests extends ESTestCase {
    public void testSimpleFormat() {
        String content = "This is a really cool highlighter. Unified highlighter gives nice snippets back. No matches here.";

        // Create two instances of CustomPassageFormatter with different values for numberOfFragments
        // to test the behavior of snippet trimming based on the numberOfFragments parameter.
        CustomPassageFormatter passageFormatterWithoutTrimming = new CustomPassageFormatter("<em>", "</em>", new DefaultEncoder(), 0);
        CustomPassageFormatter passageFormatterWithTrimming = new CustomPassageFormatter("<em>", "</em>", new DefaultEncoder(), 3);

        Passage[] passages = new Passage[3];
        String match = "highlighter";
        BytesRef matchBytesRef = new BytesRef(match);

        Passage passage1 = new Passage();
        int start = content.indexOf(match);
        int end = start + match.length();
        passage1.setStartOffset(0);
//        passage1.setEndOffset(end + 2); // let's include the whitespace at the end to make sure we trim it
        passage1.setEndOffset(content.indexOf('.', end) + 2); // Include the period and the following whitespace
        passage1.addMatch(start, end, matchBytesRef, 1);
        passages[0] = passage1;

        Passage passage2 = new Passage();
        start = content.lastIndexOf(match);
        end = start + match.length();
        passage2.setStartOffset(passage1.getEndOffset());
//        passage2.setEndOffset(end + 26);
        passage2.setEndOffset(content.indexOf('.', end) + 2); // Include the period and the following whitespace
        passage2.addMatch(start, end, matchBytesRef, 1);
        passages[1] = passage2;

        Passage passage3 = new Passage();
        passage3.setStartOffset(passage2.getEndOffset());
        passage3.setEndOffset(content.length());
        passages[2] = passage3;

        //Test case when numberOfFragments is 0
        Snippet[] fragmentsWithoutTrimming = passageFormatterWithoutTrimming.format(passages, content);
        assertThat(fragmentsWithoutTrimming, notNullValue());
        assertThat(fragmentsWithoutTrimming.length, equalTo(3));
        assertThat(fragmentsWithoutTrimming[0].getText(), equalTo("This is a really cool <em>highlighter</em>. "));
        assertThat(fragmentsWithoutTrimming[0].isHighlighted(), equalTo(true));
        assertThat(fragmentsWithoutTrimming[1].getText(), equalTo("Unified <em>highlighter</em> gives nice snippets back. "));
        assertThat(fragmentsWithoutTrimming[1].isHighlighted(), equalTo(true));
        assertThat(fragmentsWithoutTrimming[2].getText(), equalTo("No matches here."));
        assertThat(fragmentsWithoutTrimming[2].isHighlighted(), equalTo(false));

        //Test case when numberOfFragments is non-zero
        Snippet[] fragmentsWithTrimming = passageFormatterWithTrimming.format(passages, content);
        assertThat(fragmentsWithTrimming, notNullValue());
        assertThat(fragmentsWithTrimming.length, equalTo(3));
        assertThat(fragmentsWithTrimming[0].getText(), equalTo("This is a really cool <em>highlighter</em>."));
        assertThat(fragmentsWithTrimming[0].isHighlighted(), equalTo(true));
        assertThat(fragmentsWithTrimming[1].getText(), equalTo("Unified <em>highlighter</em> gives nice snippets back."));
        assertThat(fragmentsWithTrimming[1].isHighlighted(), equalTo(true));
        assertThat(fragmentsWithTrimming[2].getText(), equalTo("No matches here."));
        assertThat(fragmentsWithTrimming[2].isHighlighted(), equalTo(false));
    }

    public void testHtmlEncodeFormat() {
        String content = "<b>This is a really cool highlighter.</b> Unified highlighter gives nice snippets back.";

        CustomPassageFormatter passageFormatterWithoutTrimming = new CustomPassageFormatter("<em>", "</em>", new SimpleHTMLEncoder(), 0);
        CustomPassageFormatter passageFormatterWithTrimming = new CustomPassageFormatter("<em>", "</em>", new SimpleHTMLEncoder(), 2);

        Passage[] passages = new Passage[2];
        String match = "highlighter";
        BytesRef matchBytesRef = new BytesRef(match);

        Passage passage1 = new Passage();
        int start = content.indexOf(match);
        int end = start + match.length();
        passage1.setStartOffset(0);
//        passage1.setEndOffset(end + 6); // let's include the whitespace at the end to make sure we trim it
        passage1.setEndOffset(content.indexOf('.', end) + 6); // Include the period and the following whitespace
        passage1.addMatch(start, end, matchBytesRef, 1);
        passages[0] = passage1;

        Passage passage2 = new Passage();
        start = content.lastIndexOf(match);
        end = start + match.length();
        passage2.setStartOffset(passage1.getEndOffset());
        passage2.setEndOffset(content.length());
        passage2.addMatch(start, end, matchBytesRef, 1);
        passages[1] = passage2;

        System.out.println(passage1);
        System.out.println(passage2);

        //Test case when numberOfFragments is 0
        Snippet[] fragmentsWithoutTrimming = passageFormatterWithoutTrimming.format(passages, content);
        assertThat(fragmentsWithoutTrimming, notNullValue());
        assertThat(fragmentsWithoutTrimming.length, equalTo(2));
        assertThat(fragmentsWithoutTrimming[0].getText(), equalTo("&lt;b&gt;This is a really cool <em>highlighter</em>.&lt;&#x2F;b&gt; "));
        assertThat(fragmentsWithoutTrimming[1].getText(), equalTo("Unified <em>highlighter</em> gives nice snippets back."));

        //Test case when numberOfFragments is non-zero
        Snippet[] fragmentsWithTrimming = passageFormatterWithTrimming.format(passages, content);
        assertThat(fragmentsWithTrimming, notNullValue());
        assertThat(fragmentsWithTrimming.length, equalTo(2));
        assertThat(fragmentsWithTrimming[0].getText(), equalTo("&lt;b&gt;This is a really cool <em>highlighter</em>.&lt;&#x2F;b&gt;"));
        assertThat(fragmentsWithTrimming[1].getText(), equalTo("Unified <em>highlighter</em> gives nice snippets back."));
    }
}
