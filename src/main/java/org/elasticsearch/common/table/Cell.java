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

package org.elasticsearch.common.table;

/**
 *  A String container that supports alignment.
 */
public class Cell {
    private final String content;

    private final Align align;

    private final byte width;

    public Cell(String content) {
        this.content = content;
        this.align = Align.LEFT;
        this.width = (byte) content.length();
    }

    public Cell(String content, Align align) {
        this.content = content;
        this.align = align;
        this.width = (byte) content.length();
    }

    public Cell(String content, Align align, byte width) {
        this.content = content;
        this.align = align;
        this.width = width;
    }

    public byte width() {
        return this.width;
    }

    public Align align() {
        return this.align;
    }

    public static String pad(String orig, byte width, Align align) {
        StringBuilder s = new StringBuilder();
        byte leftOver = (byte) (width - orig.length());
        if (leftOver > 0 && align == Align.LEFT) {
            s.append(orig);
            for (byte i = 0; i < leftOver; i++) {
                s.append(" ");
            }
        } else if (leftOver > 0 && align == Align.RIGHT) {
            for (byte i = 0; i < leftOver; i++) {
                s.append(" ");
            }
            s.append(orig);
        } else {
            s.append(orig);
        }
        return s.toString();
    }

    public String toString(byte outWidth, Align outAlign) {
        return pad(content, outWidth, outAlign);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append(align());
        sb.append("]");
        sb.append(content);
        return sb.toString();
    }
}
