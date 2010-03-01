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
package org.elasticsearch.util.yaml.snakeyaml.reader;

import org.elasticsearch.util.yaml.snakeyaml.error.YAMLException;

public class ReaderException extends YAMLException {
    private static final long serialVersionUID = 8710781187529689083L;
    private String name;
    private char character;
    private int position;

    public ReaderException(String name, int position, char character, String message) {
        super(message);
        this.name = name;
        this.character = character;
        this.position = position;
    }

    @Override
    public String toString() {
        return "unacceptable character #" + Integer.toHexString((int) character).toUpperCase()
                + " " + getMessage() + "\nin \"" + name + "\", position " + position;
    }
}
