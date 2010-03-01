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
package org.elasticsearch.util.yaml.snakeyaml.error;

import org.elasticsearch.util.yaml.snakeyaml.scanner.Constant;

/**
 * It's just a record and its only use is producing nice error messages. Parser
 * does not use it for any other purposes.
 *
 * @see <a href="http://pyyaml.org/wiki/PyYAML">PyYAML</a> for more information
 */
public final class Mark {
    private String name;
    private int line;
    private int column;
    private String buffer;
    private int pointer;

    public Mark(String name, int index, int line, int column, String buffer, int pointer) {
        super();
        this.name = name;
        this.line = line;
        this.column = column;
        this.buffer = buffer;
        this.pointer = pointer;
    }

    private boolean isLineBreak(char ch) {
        return Constant.NULL_OR_LINEBR.has(ch);
    }

    public String get_snippet(int indent, int max_length) {
        if (buffer == null) {
            return null;
        }
        float half = max_length / 2 - 1;
        int start = pointer;
        String head = "";
        while ((start > 0) && (!isLineBreak(buffer.charAt(start - 1)))) {
            start -= 1;
            if (pointer - start > half) {
                head = " ... ";
                start += 5;
                break;
            }
        }
        String tail = "";
        int end = pointer;
        while ((end < buffer.length()) && (!isLineBreak(buffer.charAt(end)))) {
            end += 1;
            if (end - pointer > half) {
                tail = " ... ";
                end -= 5;
                break;
            }
        }
        String snippet = buffer.substring(start, end);
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < indent; i++) {
            result.append(" ");
        }
        result.append(head);
        result.append(snippet);
        result.append(tail);
        result.append("\n");
        for (int i = 0; i < indent + pointer - start + head.length(); i++) {
            result.append(" ");
        }
        result.append("^");
        return result.toString();
    }

    public String get_snippet() {
        return get_snippet(4, 75);
    }

    @Override
    public String toString() {
        String snippet = get_snippet();
        StringBuilder where = new StringBuilder(" in \"");
        where.append(name);
        where.append("\", line ");
        where.append(line + 1);
        where.append(", column ");
        where.append(column + 1);
        if (snippet != null) {
            where.append(":\n");
            where.append(snippet);
        }
        return where.toString();
    }

    public String getName() {
        return name;
    }

    /**
     * starts with 0
     */
    public int getLine() {
        return line;
    }

    /**
     * starts with 0
     */
    public int getColumn() {
        return column;
    }

}
