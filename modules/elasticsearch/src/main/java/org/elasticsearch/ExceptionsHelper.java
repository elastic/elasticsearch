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

package org.elasticsearch;

/**
 * @author kimchy (Shay Banon)
 */
public final class ExceptionsHelper {

    public static Throwable unwrapCause(Throwable t) {
        Throwable result = t;
        while (result instanceof ElasticSearchWrapperException) {
            result = t.getCause();
        }
        return result;
    }

    public static String detailedMessage(Throwable t, boolean newLines, int initialCounter) {
        int counter = initialCounter + 1;
        if (t.getCause() != null) {
            StringBuilder sb = new StringBuilder();
            while (t != null) {
                if (t.getMessage() != null) {
                    sb.append(t.getMessage());
                    if (!newLines) {
                        sb.append("; ");
                    }
                }
                t = t.getCause();
                if (t != null) {
                    if (newLines) {
                        sb.append("\n");
                        for (int i = 0; i < counter; i++) {
                            sb.append("\t");
                        }
                    } else {
                        sb.append("nested: ");
                    }
                }
                counter++;
            }
            return sb.toString();
        } else {
            return t.getMessage();
        }
    }
}
