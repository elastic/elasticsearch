/*
Licensed to Elasticsearch under one or more contributor
license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright
ownership. Elasticsearch licenses this file to you under
the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License.
You may obtain a copy of the License at
 *
   http://www.apache.org/licenses/LICENSE-2.0
 *
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
 */

package org.apache.lucene.search.postingshighlight;

import java.text.BreakIterator;
import java.text.CharacterIterator;

/**
 * A {@link BreakIterator} that breaks the text whenever a certain separator, provided as a constructor argument, is found.
 */
public class CustomSeparatorBreakIterator extends BreakIterator {

    private final char separator;
    private CharacterIterator text;
    private int current;

    public CustomSeparatorBreakIterator(char separator) {
        this.separator = separator;
    }

    @Override
    public int current() {
        return current;
    }

    @Override
    public int first() {
        text.setIndex(text.getBeginIndex());
        return current = text.getIndex();
    }

    @Override
    public int last() {
        text.setIndex(text.getEndIndex());
        return current = text.getIndex();
    }

    @Override
    public int next() {
        if (text.getIndex() == text.getEndIndex()) {
            return DONE;
        } else {
            return advanceForward();
        }
    }

    private int advanceForward() {
        char c;
        while( (c = text.next()) != CharacterIterator.DONE) {
            if (c == separator) {
                return current = text.getIndex() + 1;
            }
        }
        assert text.getIndex() == text.getEndIndex();
        return current = text.getIndex();
    }

    @Override
    public int following(int pos) {
        if (pos < text.getBeginIndex() || pos > text.getEndIndex()) {
            throw new IllegalArgumentException("offset out of bounds");
        } else if (pos == text.getEndIndex()) {
            // this conflicts with the javadocs, but matches actual behavior (Oracle has a bug in something)
            // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=9000909
            text.setIndex(text.getEndIndex());
            current = text.getIndex();
            return DONE;
        } else {
            text.setIndex(pos);
            current = text.getIndex();
            return advanceForward();
        }
    }

    @Override
    public int previous() {
        if (text.getIndex() == text.getBeginIndex()) {
            return DONE;
        } else {
            return advanceBackward();
        }
    }

    private int advanceBackward() {
        char c;
        while( (c = text.previous()) != CharacterIterator.DONE) {
            if (c == separator) {
                return current = text.getIndex() + 1;
            }
        }
        assert text.getIndex() == text.getBeginIndex();
        return current = text.getIndex();
    }

    @Override
    public int preceding(int pos) {
        if (pos < text.getBeginIndex() || pos > text.getEndIndex()) {
            throw new IllegalArgumentException("offset out of bounds");
        } else if (pos == text.getBeginIndex()) {
            // this conflicts with the javadocs, but matches actual behavior (Oracle has a bug in something)
            // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=9000909
            text.setIndex(text.getBeginIndex());
            current = text.getIndex();
            return DONE;
        } else {
            text.setIndex(pos);
            current = text.getIndex();
            return advanceBackward();
        }
    }

    @Override
    public int next(int n) {
        if (n < 0) {
            for (int i = 0; i < -n; i++) {
                previous();
            }
        } else {
            for (int i = 0; i < n; i++) {
                next();
            }
        }
        return current();
    }

    @Override
    public CharacterIterator getText() {
        return text;
    }

    @Override
    public void setText(CharacterIterator newText) {
        text = newText;
        current = text.getBeginIndex();
    }
}
