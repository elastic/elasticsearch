/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.painless.node.SBlock;
import org.elasticsearch.painless.node.SBreak;
import org.elasticsearch.painless.node.SCatch;
import org.elasticsearch.painless.node.SClass;
import org.elasticsearch.painless.node.SContinue;
import org.elasticsearch.painless.node.SDeclBlock;
import org.elasticsearch.painless.node.SDeclaration;
import org.elasticsearch.painless.node.SDo;
import org.elasticsearch.painless.node.SEach;
import org.elasticsearch.painless.node.SExpression;
import org.elasticsearch.painless.node.SFor;
import org.elasticsearch.painless.node.SFunction;
import org.elasticsearch.painless.node.SIf;
import org.elasticsearch.painless.node.SIfElse;
import org.elasticsearch.painless.node.SReturn;
import org.elasticsearch.painless.node.SThrow;
import org.elasticsearch.painless.node.STry;
import org.elasticsearch.painless.node.SWhile;

public interface UserTreeVisitor<Input, Output> {

    Output visitClass(SClass userClassNode, Input input);
    Output visitFunction(SFunction userFunctionNode, Input input);
    Output visitBlock(SBlock userBlockNode, Input input);
    Output visitIf(SIf userIfNode, Input input);
    Output visitIfElse(SIfElse userIfElseNode, Input input);
    Output visitWhile(SWhile userWhileNode, Input input);
    Output visitDo(SDo userDoNode, Input input);
    Output visitFor(SFor userForNode, Input input);
    Output visitEach(SEach userEachNode, Input input);
    Output visitDeclBlock(SDeclBlock userDeclBlockNode, Input input);
    Output visitDeclaration(SDeclaration userDeclarationNode, Input input);
    Output visitReturn(SReturn userReturnNode, Input input);
    Output visitExpression(SExpression userExpressionNode, Input input);
    Output visitTry(STry userTryNode, Input input);
    Output visitCatch(SCatch userCatchNode, Input input);
    Output visitThrow(SThrow userThrowNode, Input input);
    Output visitContinue(SContinue userContinueNode, Input input);
    Output visitBreak(SBreak userBreakNode, Input input);
}
