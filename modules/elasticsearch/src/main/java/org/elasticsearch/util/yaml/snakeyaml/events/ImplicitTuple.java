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
package org.elasticsearch.util.yaml.snakeyaml.events;

/**
 * The implicit flag of a scalar event is a pair of boolean values that indicate
 * if the tag may be omitted when the scalar is emitted in a plain and non-plain
 * style correspondingly.
 *
 * @see http://pyyaml.org/wiki/PyYAMLDocumentation#Events
 * @see <a href="http://pyyaml.org/wiki/PyYAML">PyYAML</a> for more information
 */
public class ImplicitTuple {
    private final boolean plain;
    private final boolean nonPlain;

    public ImplicitTuple(boolean plain, boolean nonplain) {
        this.plain = plain;
        this.nonPlain = nonplain;
    }

    /**
     * @return true when tag may be omitted when the scalar is emitted in a
     *         plain style.
     */
    public boolean isFirst() {
        return plain;
    }

    /**
     * @return true when tag may be omitted when the scalar is emitted in a
     *         non-plain style.
     */
    public boolean isSecond() {
        return nonPlain;
    }

    public boolean bothFalse() {
        return !plain && !nonPlain;
    }

    @Override
    public String toString() {
        return "implicit=[" + plain + ", " + nonPlain + "]";
    }
}
