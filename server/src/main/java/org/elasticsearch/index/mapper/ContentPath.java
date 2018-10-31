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

package org.elasticsearch.index.mapper;

public final class ContentPath {

    private static final char DELIMITER = '.';

    private final StringBuilder sb;

    private final int offset;

    private int index = 0;

    private String[] path = new String[10];

    public ContentPath() {
        this(0);
    }

    /**
     * Constructs a json path with an offset. The offset will result an {@code offset}
     * number of path elements to not be included in {@link #pathAsText(String)}.
     */
    public ContentPath(int offset) {
        this.sb = new StringBuilder();
        this.offset = offset;
        this.index = 0;
    }

    public void add(String name) {
        path[index++] = name;
        if (index == path.length) { // expand if needed
            String[] newPath = new String[path.length + 10];
            System.arraycopy(path, 0, newPath, 0, path.length);
            path = newPath;
        }
    }

    public void remove() {
        path[index--] = null;
    }

    public String pathAsText(String name) {
        sb.setLength(0);
        for (int i = offset; i < index; i++) {
            sb.append(path[i]).append(DELIMITER);
        }
        sb.append(name);
        return sb.toString();
    }

    public int length() {
        return index;
    }
}
