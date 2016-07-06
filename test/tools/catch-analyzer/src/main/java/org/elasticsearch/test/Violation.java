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

package org.elasticsearch.test;

/** some kind of problem with exception handling, with associated line number info */
class Violation implements Comparable<Violation> {
    final Kind kind;
    final int lineNumber;
    final int secondaryLineNumber;
    
    Violation(Kind kind, int lineNumber, int secondaryLineNumber) {
        this.kind = kind;
        this.lineNumber = lineNumber;
        this.secondaryLineNumber = secondaryLineNumber;
    }
    
    int getLineNumber() {
        return lineNumber;
    }

    static enum Kind {        
        ESCAPES_WITHOUT_THROWING_ANYTHING("Control flow escapes with no exception"),
        THROWS_SOMETHING_ELSE_BUT_LOSES_ORIGINAL("Throws a different exception, losing the original"),
        ANNOTATED_AS_SWALLOWER_BUT_HAS_NO_PROBLEMS("Annotated with @SwallowsException but has no problems");
        
        final String description;
        Kind(String description) {
            this.description = description;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + lineNumber;
        result = prime * result + secondaryLineNumber;
        result = prime * result + kind.hashCode();

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        Violation other = (Violation) obj;
        if (lineNumber != other.lineNumber) return false;
        if (secondaryLineNumber != other.secondaryLineNumber) return false;
        if (kind != other.kind) return false;
        return true;
    }

    @Override
    public int compareTo(Violation other) {
        int cmp = Integer.compare(lineNumber, other.lineNumber);
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(secondaryLineNumber, other.secondaryLineNumber);
        if (cmp != 0) {
            return cmp;
        }
        return kind.compareTo(other.kind);
    }
}
