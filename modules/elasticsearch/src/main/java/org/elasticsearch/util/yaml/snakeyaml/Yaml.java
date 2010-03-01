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
package org.elasticsearch.util.yaml.snakeyaml;

import org.elasticsearch.util.yaml.snakeyaml.events.Event;
import org.elasticsearch.util.yaml.snakeyaml.nodes.Node;
import org.elasticsearch.util.yaml.snakeyaml.reader.UnicodeReader;
import org.elasticsearch.util.yaml.snakeyaml.resolver.Resolver;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;

/**
 * Public YAML interface. Each Thread must have its own instance.
 */
public class Yaml {
    private final Loader loader;
    private final Resolver resolver;
    private String name;

    /**
     * Create Yaml instance. It is safe to create a few instances and use them
     * in different Threads.
     *
     * @param loader   Loader to parse incoming documents
     * @param resolver Resolver to detect implicit type
     */
    public Yaml(Loader loader, Resolver resolver) {
        this.loader = loader;
        loader.setAttached();
        this.resolver = resolver;
        this.loader.setResolver(resolver);
        this.name = "Yaml:" + System.identityHashCode(this);
    }

    public Yaml() {
        this(new Loader(), new Resolver());
    }

    /**
     * Parse the first YAML document in a String and produce the corresponding
     * Java object. (Because the encoding in known BOM is not respected.)
     *
     * @param yaml YAML data to load from (BOM must not be present)
     * @return parsed object
     */
    public Object load(String yaml) {
        return loader.load(new StringReader(yaml));
    }

    /**
     * Parse the first YAML document in a stream and produce the corresponding
     * Java object.
     *
     * @param io data to load from (BOM is respected and removed)
     * @return parsed object
     */
    public Object load(InputStream io) {
        return loader.load(new UnicodeReader(io));
    }

    /**
     * Parse the first YAML document in a stream and produce the corresponding
     * Java object.
     *
     * @param io data to load from (BOM must not be present)
     * @return parsed object
     */
    public Object load(Reader io) {
        return loader.load(io);
    }

    /**
     * Parse all YAML documents in a String and produce corresponding Java
     * objects.
     *
     * @param yaml YAML data to load from (BOM must not be present)
     * @return an iterator over the parsed Java objects in this String in proper
     *         sequence
     */
    public Iterable<Object> loadAll(Reader yaml) {
        return loader.loadAll(yaml);
    }

    /**
     * Parse all YAML documents in a String and produce corresponding Java
     * objects. (Because the encoding in known BOM is not respected.)
     *
     * @param yaml YAML data to load from (BOM must not be present)
     * @return an iterator over the parsed Java objects in this String in proper
     *         sequence
     */
    public Iterable<Object> loadAll(String yaml) {
        return loadAll(new StringReader(yaml));
    }

    /**
     * Parse all YAML documents in a stream and produce corresponding Java
     * objects.
     *
     * @param yaml YAML data to load from (BOM is respected and ignored)
     * @return an iterator over the parsed Java objects in this stream in proper
     *         sequence
     */
    public Iterable<Object> loadAll(InputStream yaml) {
        return loadAll(new UnicodeReader(yaml));
    }

    /**
     * Parse the first YAML document in a stream and produce the corresponding
     * representation tree.
     *
     * @param io stream of a YAML document
     * @return parsed root Node for the specified YAML document
     */
    public Node compose(Reader io) {
        return loader.compose(io);
    }

    /**
     * Parse all YAML documents in a stream and produce corresponding
     * representation trees.
     *
     * @param io stream of YAML documents
     * @return parsed root Nodes for all the specified YAML documents
     */
    public Iterable<Node> composeAll(Reader io) {
        return loader.composeAll(io);
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Get a meaningful name. It simplifies debugging in a multi-threaded
     * environment. If nothing is set explicitly the address of the instance is
     * returned.
     *
     * @return human readable name
     */
    public String getName() {
        return name;
    }

    /**
     * Set a meaningful name to be shown in toString()
     *
     * @param name human readable name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Parse a YAML stream and produce parsing events.
     *
     * @param yaml YAML document(s)
     * @return parsed events
     */
    public Iterable<Event> parse(Reader yaml) {
        return loader.parse(yaml);
    }
}
