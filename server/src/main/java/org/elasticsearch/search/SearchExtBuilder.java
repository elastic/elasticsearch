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

package org.elasticsearch.search;

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.SearchPlugin.SearchExtSpec;

/**
 * Intermediate serializable representation of a search ext section. To be subclassed by plugins that support
 * a custom section as part of a search request, which will be provided within the ext element.
 * Any state needs to be serialized as part of the {@link Writeable#writeTo(StreamOutput)} method and
 * read from the incoming stream, usually done adding a constructor that takes {@link StreamInput} as
 * an argument.
 *
 * Registration happens through {@link SearchPlugin#getSearchExts()}, which also needs a {@link CheckedFunction} that's able to parse
 * the incoming request from the REST layer into the proper {@link SearchExtBuilder} subclass.
 *
 * {@link #getWriteableName()} must return the same name as the one used for the registration
 * of the {@link SearchExtSpec}.
 *
 * @see SearchExtSpec
 */
public abstract class SearchExtBuilder implements NamedWriteable, ToXContentFragment {

    public abstract int hashCode();

    public abstract boolean equals(Object obj);
}
