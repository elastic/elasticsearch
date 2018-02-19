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

import java.util.ArrayList;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.RunAutomaton;

public class AutomatonSkeleton {

	public static void main(String [] args) {

		// The different regexes that are part of the language
		String[] regexes = {
				"/a/b/c",
				"a/d/g",
				"x/b/c",
				"a/x/.+",
				"a/b/.+",
				".+/.+/x",
				"\\{index\\}/insert/\\{docId\\}" // Notice that the regex needs \\ before '{' or '}'
		};

		// Create automatons from the regexes
		ArrayList<Automaton> automata = new ArrayList<Automaton>();
		for (String s : regexes) {
			automata.add(new RegExp(s).toAutomaton());
		}

		// Create a runnable automaton from the generated automata
		Automaton automaton = Operations.union(automata);
		Automaton minified = MinimizationOperations.minimize(automaton, automaton.getNumStates());
		RunAutomaton runnable = new ByteRunAutomaton(minified);

		// The Strings we will check if they are part of the language
		String[] tests = {
				"/a/b/c",
				"/a/b/no_match",
				"a/d/g",
				"a/d/no_match",
				"x/b/c",
				"x/b/no_match",
				"a/x/anything_goes",
				"a/no_match/anything_goes",
				"a/b/anything_goes",
				"a/no_match/anything_goes",
				"anything_goes/anything_goes/x",
				"anything_goes/anything_goes/no_match",
				"{index}/insert/{docId}", // The test does NOT need \\ before '{' or '}'
				"myIndex/insert/myID" // Notice that this is NOT a match
		};

		// Test one String at a time
		for (String s : tests) {
			int state = 0;
			for (char c : s.toCharArray()) {
				// Get the next state by going from the current state over the transition called 'c'
				state = runnable.step(state, c);
				// -1 means there was no such transition and the current String is NOT part of the language
				if (state == -1) {
					break;
				}
			}
			System.out.println(state != -1 && runnable.isAccept(state) ? s + " is part of the language" : s + " is NOT part of the language");
		}
    }
}
