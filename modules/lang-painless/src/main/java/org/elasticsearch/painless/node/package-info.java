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

/**
 * A painless tree is composed of the node classes found in this package.
 * <p>
 * The following are the types of nodes:
 * A* (abstract) - These are the abstract nodes that are the superclasses for the other types.
 * I* (interface) -- Thse are marker interfaces to denote a property of the node.
 * S* (statement) - These are nodes that represent a statement in Painless.  These are the highest level nodes.
 * E* (expression) - These are nodes that represent an expression in Painless.  These are the middle level nodes.
 * L* (link) - These are nodes that represent a piece of a variable/method chain.  The are the lowest level nodes.
 * <p>
 * The following is a brief description of each node:
 * {@link org.elasticsearch.painless.node.AExpression} - The superclass for all E* (expression) nodes.
 * {@link org.elasticsearch.painless.node.ALink} - The superclass for all L* (link) nodes.
 * {@link org.elasticsearch.painless.node.ANode} - The superclass for all other nodes.
 * {@link org.elasticsearch.painless.node.AStatement} - The superclass for all S* (statement) nodes.
 * {@link org.elasticsearch.painless.node.EBinary} - Represents a binary math expression.
 * {@link org.elasticsearch.painless.node.EBool} - Represents a boolean expression.
 * {@link org.elasticsearch.painless.node.EBoolean} - Represents a boolean constant.
 * {@link org.elasticsearch.painless.node.ECapturingFunctionRef} - Represents a function reference (capturing).
 * {@link org.elasticsearch.painless.node.ECast} - Represents an implicit cast in most cases.  (Internal only.)
 * {@link org.elasticsearch.painless.node.EChain} - Represents the entirety of a variable/method chain for read/write operations.
 * {@link org.elasticsearch.painless.node.EComp} - Represents a comparison expression.
 * {@link org.elasticsearch.painless.node.EConditional} - Represents a conditional expression.
 * {@link org.elasticsearch.painless.node.EConstant} - Represents a constant.  (Internal only.)
 * {@link org.elasticsearch.painless.node.EDecimal} - Represents a decimal constant.
 * {@link org.elasticsearch.painless.node.EExplicit} - Represents an explicit cast.
 * {@link org.elasticsearch.painless.node.EFunctionRef} - Represents a function reference (non-capturing).
 * {@link org.elasticsearch.painless.node.ENull} - Represents a null constant.
 * {@link org.elasticsearch.painless.node.ENumeric} - Represents a non-decimal numeric constant.
 * {@link org.elasticsearch.painless.node.EUnary} - Represents a unary math expression.
 * {@link org.elasticsearch.painless.node.IDefLink} - A marker interface for all LDef* (link) nodes.
 * {@link org.elasticsearch.painless.node.LArrayLength} - Represents an array length field load.
 * {@link org.elasticsearch.painless.node.LBrace} - Represents an array load/store or defers to possible shortcuts.
 * {@link org.elasticsearch.painless.node.LCallInvoke} - Represents a method call or defers to a def call.
 * {@link org.elasticsearch.painless.node.LCallLocal} - Represents a user-defined call.
 * {@link org.elasticsearch.painless.node.LCast} - Represents a cast made in a variable/method chain.
 * {@link org.elasticsearch.painless.node.LDefArray} - Represents an array load/store or shortcut on a def type.  (Internal only.)
 * {@link org.elasticsearch.painless.node.LDefCall} - Represents a method call made on a def type. (Internal only.)
 * {@link org.elasticsearch.painless.node.LDefField} - Represents a field load/store or shortcut on a def type.  (Internal only.)
 * {@link org.elasticsearch.painless.node.LField} - Represents a field load/store or defers to a possible shortcuts.
 * {@link org.elasticsearch.painless.node.LListShortcut} - Represents a list load/store shortcut.  (Internal only.)
 * {@link org.elasticsearch.painless.node.LMapShortcut} - Represents a map load/store shortcut. (Internal only.)
 * {@link org.elasticsearch.painless.node.LNewArray} - Represents an array instantiation.
 * {@link org.elasticsearch.painless.node.LNewObj} - Represents and object instantiation.
 * {@link org.elasticsearch.painless.node.LShortcut} - Represents a field load/store shortcut.  (Internal only.)
 * {@link org.elasticsearch.painless.node.LStatic} - Represents a static type target.
 * {@link org.elasticsearch.painless.node.LString} - Represents a string constant.
 * {@link org.elasticsearch.painless.node.LVariable} - Represents a variable load/store.
 * {@link org.elasticsearch.painless.node.SBlock} - Represents a set of statements as a branch of control-flow.
 * {@link org.elasticsearch.painless.node.SBreak} - Represents a break statement.
 * {@link org.elasticsearch.painless.node.SCatch} - Represents a catch block as part of a try-catch block.
 * {@link org.elasticsearch.painless.node.SContinue} - Represents a continue statement.
 * {@link org.elasticsearch.painless.node.SDeclaration} - Represents a single variable declaration.
 * {@link org.elasticsearch.painless.node.SDeclBlock} - Represents a series of declarations.
 * {@link org.elasticsearch.painless.node.SDo} - Represents a do-while loop.
 * {@link org.elasticsearch.painless.node.SEach} - Represents a for each loop shortcut for iterables.
 * {@link org.elasticsearch.painless.node.SExpression} - Represents the top-level node for an expression as a statement.
 * {@link org.elasticsearch.painless.node.SFor} - Represents a for loop.
 * {@link org.elasticsearch.painless.node.SFunction} - Represents a user-defined function.
 * {@link org.elasticsearch.painless.node.SIf} - Represents an if block.
 * {@link org.elasticsearch.painless.node.SIfElse} - Represents an if/else block.
 * {@link org.elasticsearch.painless.node.SReturn} - Represents a return statement.
 * {@link org.elasticsearch.painless.node.SSource} - The root of all Painless trees.  Contains a series of statements.
 * {@link org.elasticsearch.painless.node.SThrow} - Represents a throw statement.
 * {@link org.elasticsearch.painless.node.STry} - Represents the try block as part of a try-catch block.
 * {@link org.elasticsearch.painless.node.SWhile} - Represents a while loop.
 * <p>
 * Note that internal nodes are generated during the analysis phase by modifying the tree on-the-fly
 * for clarity of development and convenience during the writing phase.
 * <p>
 * All Painless trees must start with an SSource node at the root.  Each node has a constructor that requires
 * all of its values and children be passed in at the time of instantiation.  This means that Painless trees
 * are build bottom-up; however, this helps enforce tree structure to be correct and fits naturally with a
 * standard recurvise-descent parser.
 * <p>
 * Generally, statement nodes have member data that evaluate legal control-flow during the analysis phase.
 * The typical order for statement nodes is for each node to call analyze on it's children during the analysis phase
 * and write on it's children during the writing phase.
 * <p>
 * Generally, expression nodes have member data that evaluate static types.  The typical order for an expression node
 * during the analysis phase looks like the following:
 * {@code
 * For known expected types:
 *
 * expression.child.expected = expectedType      // set the known expected type
 *
 * expression.child.analyze(...)                 // analyze the child node to set the child's actual type
 *
 * expression.child = expression.child.cast(...) // add an implicit cast node if the child node's
 *                                               // actual type is not the expected type and set the
 *                                               // expression's child to the implicit cast node
 *
 * For unknown expected types that need promotion:
 *
 * expression.child.analyze(...)                 // analyze the child node to set the child's actual type
 *
 * Type promote = Caster.promote(...)            // get the promotion type for the child based on
 *                                               // the current operation and child's actual type
 *
 * expression.child.expected = promote           // set the expected type to the promotion type
 *
 * expression.child = expression.child.cast(...) // add an implicit cast node if the child node's
 *                                               // actual type is not the expected type and set the
 *                                               // expression's child to the implicit cast node
 * }
 * Expression nodes just call each child during the writing phase.
 * <p>
 * Generally, link nodes have member data that help keep track of items necessary to do a
 * load/store on a variable/field/method.  Analysis of link nodes happens in a chain node
 * where each link node will be analysed with the chain node acting as a bridge to pass the
 * previous link's after type to the next link's before type.  Upon analysis completion, a link
 * will return either itself or another link node depending on if a shortcut or def type was found.
 * Cast nodes as links will return null and be removed from the chain node if the cast is
 * unnecessary.  Link nodes have three methods for writing -- write, load, and store.  The write
 * method is always once called before a load/store to give links a chance to write any values
 * such as array indices before the load/store happens.  Load is called to read a link node, and
 * store is called to write a link node.  Note that store will only ever be called on the final
 * link node in a chain, all previous links will be considered loads.
 */
package org.elasticsearch.painless.node;
