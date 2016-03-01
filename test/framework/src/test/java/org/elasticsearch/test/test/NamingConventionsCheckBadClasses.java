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

package org.elasticsearch.test.test;

import org.elasticsearch.test.ESTestCase;

import junit.framework.TestCase;

/**
 * These inner classes all fail the NamingConventionsCheck. They have to live in the tests or else they won't be scanned.
 */
public class NamingConventionsCheckBadClasses {
    public static final class NotImplementingTests {
    }

    public static final class WrongName extends ESTestCase {
    }

    public static abstract class DummyAbstractTests extends ESTestCase {
    }

    public interface DummyInterfaceTests {
    }

    public static final class InnerTests extends ESTestCase {
    }

    public static final class WrongNameTheSecond extends ESTestCase {
    }

    public static final class PlainUnit extends TestCase {
    }
}
