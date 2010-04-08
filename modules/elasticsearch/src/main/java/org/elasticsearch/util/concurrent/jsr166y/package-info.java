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

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */


/**
 * Preview versions of classes targeted for Java 7.  Includes a
 * fine-grained parallel computation framework: ForkJoinTasks and
 * their related support classes provide a very efficient basis for
 * obtaining platform-independent parallel speed-ups of
 * computation-intensive operations.  They are not a full substitute
 * for the kinds of arbitrary processing supported by Executors or
 * Threads. However, when applicable, they typically provide
 * significantly greater performance on multiprocessor platforms.
 *
 * <p>Candidates for fork/join processing mainly include those that
 * can be expressed using parallel divide-and-conquer techniques: To
 * solve a problem, break it in two (or more) parts, and then solve
 * those parts in parallel, continuing on in this way until the
 * problem is too small to be broken up, so is solved directly.  The
 * underlying <em>work-stealing</em> framework makes subtasks
 * available to other threads (normally one per CPU), that help
 * complete the tasks.  In general, the most efficient ForkJoinTasks
 * are those that directly implement this algorithmic design pattern.
 */
package org.elasticsearch.util.concurrent.jsr166y;
