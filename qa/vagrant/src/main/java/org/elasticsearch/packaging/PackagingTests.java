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

package org.elasticsearch.packaging;

import org.elasticsearch.packaging.test.DefaultWindowsServiceTests;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
    /*DefaultLinuxTarTests.class,
    OssLinuxTarTests.class,
    DefaultWindowsZipTests.class,
    OssWindowsZipTests.class,
    DefaultRpmBasicTests.class,
    OssRpmBasicTests.class,
    DefaultDebBasicTests.class,
    OssDebBasicTests.class,
    DefaultDebPreservationTests.class,
    OssDebPreservationTests.class,
    DefaultRpmPreservationTests.class,
    OssRpmPreservationTests.class,*/
    DefaultWindowsServiceTests.class/*,
    OssWindowsServiceTests.class,
    DefaultNoJdkLinuxTarTests.class,
    OssNoJdkLinuxTarTests.class,
    DefaultNoJdkWindowsZipTests.class,
    OssNoJdkWindowsZipTests.class,
    DefaultNoJdkRpmBasicTests.class,
    OssNoJdkRpmBasicTests.class,
    DefaultNoJdkDebBasicTests.class,
    OssNoJdkDebBasicTests.class*/
})
public class PackagingTests {}
