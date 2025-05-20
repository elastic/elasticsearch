/*
 * @notice
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Sourced from: https://github.com/apache/lucene/blob/main/lucene/core/src/test/org/apache/lucene/util/automaton/TestMinimize.java
 */

package org.elasticsearch.lucene.util.automaton;

import org.apache.lucene.tests.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.test.ESTestCase;

import static org.apache.lucene.tests.util.automaton.AutomatonTestUtil.subsetOf;

public class MinimizationOperationsTests extends ESTestCase {

    /** the minimal and non-minimal are compared to ensure they are the same. */
    public void testBasic() {
        int num = atLeast(200);
        for (int i = 0; i < num; i++) {
            Automaton a = AutomatonTestUtil.randomAutomaton(random());
            Automaton la = Operations.determinize(Operations.removeDeadStates(a), Integer.MAX_VALUE);
            Automaton lb = MinimizationOperations.minimize(a, Integer.MAX_VALUE);
            assertTrue(sameLanguage(la, lb));
        }
    }

    /**
     * compare minimized against minimized with a slower, simple impl. we check not only that they are
     * the same, but that #states/#transitions are the same.
     */
    public void testAgainstBrzozowski() {
        int num = atLeast(200);
        for (int i = 0; i < num; i++) {
            Automaton a = AutomatonTestUtil.randomAutomaton(random());
            a = AutomatonTestUtil.minimizeSimple(a);
            Automaton b = MinimizationOperations.minimize(a, Integer.MAX_VALUE);
            assertTrue(sameLanguage(a, b));
            assertEquals(a.getNumStates(), b.getNumStates());
            int numStates = a.getNumStates();

            int sum1 = 0;
            for (int s = 0; s < numStates; s++) {
                sum1 += a.getNumTransitions(s);
            }
            int sum2 = 0;
            for (int s = 0; s < numStates; s++) {
                sum2 += b.getNumTransitions(s);
            }

            assertEquals(sum1, sum2);
        }
    }

    private static boolean sameLanguage(Automaton a1, Automaton a2) {
        if (a1 == a2) {
            return true;
        }
        return subsetOf(a2, a1) && subsetOf(a1, a2);
    }
}
