/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logstashbridge;

import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.Closeable;

import static org.elasticsearch.logstashbridge.DucktypeMatchers.instancesQuackLike;
import static org.elasticsearch.logstashbridge.DucktypeMatchers.staticallyQuacksLike;

public class CoreBridgeTests extends ESTestCase {

    public void testTimeValueAPIStability() {
        interface TimeValueStaticAPI {
            TimeValue timeValueMillis(long millis);
        }
        assertThat(TimeValue.class, staticallyQuacksLike(TimeValueStaticAPI.class));
    }

    public void testIOUtilsAPIStability() {
        interface IOUtilsStaticAPI {
            void closeWhileHandlingException(Iterable<? extends Closeable> objects);

            void closeWhileHandlingException(Closeable closeable);
        }
        assertThat(IOUtils.class, staticallyQuacksLike(IOUtilsStaticAPI.class));
    }

    public void testStringsAPIStability() {
        interface StringsStaticAPI {
            String format(String format, Object... args);
        }
        assertThat(Strings.class, staticallyQuacksLike(StringsStaticAPI.class));
    }

    public void testReleasableAPIStability() {
        interface ReleasableInstanceAPI {
            void close();
        }
        assertThat(Releasable.class, instancesQuackLike(ReleasableInstanceAPI.class));
    }
}
