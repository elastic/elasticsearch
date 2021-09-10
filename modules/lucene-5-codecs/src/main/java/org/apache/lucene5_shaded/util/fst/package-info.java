/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Finite state transducers
 * <p>
 * This package implements <a href="http://en.wikipedia.org/wiki/Finite_state_transducer">
 * Finite State Transducers</a> with the following characteristics:
 * <ul>
 *    <li>Fast and low memory overhead construction of the minimal FST 
 *        (but inputs must be provided in sorted order)</li>
 *    <li>Low object overhead and quick deserialization (byte[] representation)</li>
 *    <li>Optional two-pass compression: {@link org.apache.lucene5_shaded.util.fst.FST#pack FST.pack()}</li>
 *    <li>{@link org.apache.lucene5_shaded.util.fst.Util#getByOutput Lookup-by-output} when the
 *        outputs are in sorted order (e.g., ordinals or file pointers)</li>
 *    <li>Pluggable {@link org.apache.lucene5_shaded.util.fst.Outputs Outputs} representation</li>
 *    <li>{@link org.apache.lucene5_shaded.util.fst.Util#shortestPaths N-shortest-paths} search by
 *        weight</li>
 *    <li>Enumerators ({@link org.apache.lucene5_shaded.util.fst.IntsRefFSTEnum IntsRef} and {@link org.apache.lucene5_shaded.util.fst.BytesRefFSTEnum BytesRef}) that behave like {@link java.util.SortedMap SortedMap} iterators
 * </ul>
 * <p>
 * FST Construction example:
 * <pre class="prettyprint">
 *     // Input values (keys). These must be provided to Builder in Unicode sorted order!
 *     String inputValues[] = {"cat", "dog", "dogs"};
 *     long outputValues[] = {5, 7, 12};
 *     
 *     PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
 *     Builder&lt;Long&gt; builder = new Builder&lt;Long&gt;(INPUT_TYPE.BYTE1, outputs);
 *     BytesRef scratchBytes = new BytesRef();
 *     IntsRefBuilder scratchInts = new IntsRefBuilder();
 *     for (int i = 0; i &lt; inputValues.length; i++) {
 *       scratchBytes.copyChars(inputValues[i]);
 *       builder.add(Util.toIntsRef(scratchBytes, scratchInts), outputValues[i]);
 *     }
 *     FST&lt;Long&gt; fst = builder.finish();
 * </pre>
 * Retrieval by key:
 * <pre class="prettyprint">
 *     Long value = Util.get(fst, new BytesRef("dog"));
 *     System.out.println(value); // 7
 * </pre>
 * Retrieval by value:
 * <pre class="prettyprint">
 *     // Only works because outputs are also in sorted order
 *     IntsRef key = Util.getByOutput(fst, 12);
 *     System.out.println(Util.toBytesRef(key, scratchBytes).utf8ToString()); // dogs
 * </pre>
 * Iterate over key-value pairs in sorted order:
 * <pre class="prettyprint">
 *     // Like TermsEnum, this also supports seeking (advance)
 *     BytesRefFSTEnum&lt;Long&gt; iterator = new BytesRefFSTEnum&lt;Long&gt;(fst);
 *     while (iterator.next() != null) {
 *       InputOutput&lt;Long&gt; mapEntry = iterator.current();
 *       System.out.println(mapEntry.input.utf8ToString());
 *       System.out.println(mapEntry.output);
 *     }
 * </pre>
 * N-shortest paths by weight:
 * <pre class="prettyprint">
 *     Comparator&lt;Long&gt; comparator = new Comparator&lt;Long&gt;() {
 *       public int compare(Long left, Long right) {
 *         return left.compareTo(right);
 *       }
 *     };
 *     Arc&lt;Long&gt; firstArc = fst.getFirstArc(new Arc&lt;Long&gt;());
 *     MinResult&lt;Long&gt; paths[] = Util.shortestPaths(fst, firstArc, comparator, 2);
 *     System.out.println(Util.toBytesRef(paths[0].input, scratchBytes).utf8ToString()); // cat
 *     System.out.println(paths[0].output); // 5
 *     System.out.println(Util.toBytesRef(paths[1].input, scratchBytes).utf8ToString()); // dog
 *     System.out.println(paths[1].output); // 7
 * </pre>
 */
package org.apache.lucene5_shaded.util.fst;
