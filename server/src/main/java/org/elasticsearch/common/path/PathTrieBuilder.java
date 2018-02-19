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

package org.elasticsearch.common.path;

import java.util.function.BiFunction;

public class PathTrieBuilder<T>{

    public interface Decoder {
        String decode(String value);
    }

	private final Decoder decoder;
	private final TrieNode<T> root;
	private byte[] rootValue;
	private FSTRepresentation<T> representation;

	public PathTrieBuilder(Decoder decoder){
		this.representation = new FSTRepresentation<T>();
		this.decoder = decoder;
		root = new TrieNode<T>(PathTrie.SEPARATOR, null, PathTrie.WILDCARD);
	}


	//Idea of how to structure this:
	/* I will probably have to move TrieNode to its own class.
	 * This means that it would be beneficial if TrieNode was
	 * immutable as well. Therefore, also create a TrieNodeBuilder
	 * that builds individual TrieNodes inside the PathTrieBuilder.
	 * NOTE: Might not be necessary if we can access public inner classes.
	 */

	//Change the PathTrie so that the constructor takes a TrieNode that we will build up

	//have insert, insert or update here, not in PathTrie
	//pathtrie should only contain the Iterator class + call method

	public void insert(String path, T value) {
        String[] strings = path.split(PathTrie.SEPARATOR);
        if (strings.length == 0) {
            if (rootValue != null) {
                throw new IllegalArgumentException("Path [/] already has a value [" + rootValue + "]");
            }
            rootValue = representation.toBytes(value);
            return;
        }
        int index = 0;
        // Supports initial delimiter.
        if (strings.length > 0 && strings[0].isEmpty()) {
            index = 1;
        }
        root.insert(strings, index, value);
    }

    /**
     * Insert a value for the given path. If the path already exists, replace the value with:
     * <pre>
     * value = updater.apply(oldValue, newValue);
     * </pre>
     * allowing the value to be updated if desired.
     */
    public void insertOrUpdate(String path, T value, BiFunction<T, T, T> updater) {
        String[] strings = path.split(PathTrie.SEPARATOR);
        if (strings.length == 0) {
            if (rootValue != null) {
            	T currentValue = representation.fromBytes(rootValue);
            	T updatedValue = updater.apply(currentValue, value);
                rootValue = representation.toBytes(updatedValue);
            } else {
                rootValue = representation.toBytes(value);
            }
            return;
        }
        int index = 0;
        // Supports initial delimiter.
        if (strings.length > 0 && strings[0].isEmpty()) {
            index = 1;
        }
        root.insertOrUpdate(strings, index, value, updater);
    }


    /**
     * Creates a PathTrie object based
     *
    **/
    public PathTrie<T> createpathTrie(){
    	return new PathTrie<T>(decoder, root, rootValue);
    }
}
