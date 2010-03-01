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

/**
 * @see <a href="http://pyyaml.org/wiki/PyYAML">PyYAML</a> for more information
 */
public class MarkedYAMLException extends YAMLException {

    private static final long serialVersionUID = -9119388488683035101L;
    private String context;
    private Mark contextMark;
    private String problem;
    private Mark problemMark;
    private String note;

    protected MarkedYAMLException(String context, Mark contextMark, String problem,
                                  Mark problemMark, String note) {
        this(context, contextMark, problem, problemMark, note, null);
    }

    protected MarkedYAMLException(String context, Mark contextMark, String problem,
                                  Mark problemMark, String note, Throwable cause) {
        super(context + "; " + problem, cause);
        this.context = context;
        this.contextMark = contextMark;
        this.problem = problem;
        this.problemMark = problemMark;
        this.note = note;
    }

    protected MarkedYAMLException(String context, Mark contextMark, String problem, Mark problemMark) {
        this(context, contextMark, problem, problemMark, null, null);
    }

    protected MarkedYAMLException(String context, Mark contextMark, String problem,
                                  Mark problemMark, Throwable cause) {
        this(context, contextMark, problem, problemMark, null, cause);
    }

    @Override
    public String toString() {
        StringBuilder lines = new StringBuilder();
        if (context != null) {
            lines.append(context);
            lines.append("\n");
        }
        if (contextMark != null
                && (problem == null || problemMark == null
                || (contextMark.getName() != problemMark.getName())
                || (contextMark.getLine() != problemMark.getLine()) || (contextMark
                .getColumn() != problemMark.getColumn()))) {
            lines.append(contextMark.toString());
            lines.append("\n");
        }
        if (problem != null) {
            lines.append(problem);
            lines.append("\n");
        }
        if (problemMark != null) {
            lines.append(problemMark.toString());
            lines.append("\n");
        }
        if (note != null) {
            lines.append(note);
            lines.append("\n");
        }
        return lines.toString();
    }

    public String getContext() {
        return context;
    }

    public Mark getContextMark() {
        return contextMark;
    }

    public String getProblem() {
        return problem;
    }

    public Mark getProblemMark() {
        return problemMark;
    }
}
