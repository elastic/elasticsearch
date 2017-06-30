/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import java.util.Locale;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.DebugContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ExplainContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.SessionResetContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.SessionSetContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ShowColumnsContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ShowFunctionsContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ShowSchemasContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ShowSessionContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ShowTablesContext;
import org.elasticsearch.xpack.sql.plan.TableIdentifier;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.plan.logical.command.Debug;
import org.elasticsearch.xpack.sql.plan.logical.command.Explain;
import org.elasticsearch.xpack.sql.plan.logical.command.SessionReset;
import org.elasticsearch.xpack.sql.plan.logical.command.SessionSet;
import org.elasticsearch.xpack.sql.plan.logical.command.ShowColumns;
import org.elasticsearch.xpack.sql.plan.logical.command.ShowFunctions;
import org.elasticsearch.xpack.sql.plan.logical.command.ShowSchemas;
import org.elasticsearch.xpack.sql.plan.logical.command.ShowSession;
import org.elasticsearch.xpack.sql.plan.logical.command.ShowTables;
import org.elasticsearch.xpack.sql.tree.Location;

abstract class CommandBuilder extends LogicalPlanBuilder {

    @Override
    public Command visitDebug(DebugContext ctx) {
        Location loc = source(ctx);
        if (ctx.FORMAT().size() > 1) {
            throw new ParsingException(loc, "Debug FORMAT should be specified at most once");
        }
        if (ctx.PLAN().size() > 1) {
            throw new ParsingException(loc, "Debug PLAN should be specified at most once");
        }

        Debug.Type type = null;

        if (ctx.type != null) {
            if (ctx.type.getType() == SqlBaseLexer.ANALYZED) {
                type = Debug.Type.ANALYZED;
            }
            else {
                type = Debug.Type.OPTIMIZED;
            }
        }
        Debug.Format format = (ctx.format != null && ctx.format.getType() == SqlBaseLexer.GRAPHVIZ ? Debug.Format.GRAPHVIZ : Debug.Format.TEXT);

        return new Debug(loc, plan(ctx.statement()), type, format);
    }
    
    
    @Override
    public Command visitExplain(ExplainContext ctx) {
        Location loc = source(ctx);
        if (ctx.PLAN().size() > 1) {
            throw new ParsingException(loc, "Explain TYPE should be specified at most once");
        }
        if (ctx.FORMAT().size() > 1) {
            throw new ParsingException(loc, "Explain FORMAT should be specified at most once");
        }
        if (ctx.VERIFY().size() > 1) {
            throw new ParsingException(loc, "Explain VERIFY should be specified at most once");
        }

        Explain.Type type = null;

        if (ctx.type != null) {
            switch (ctx.type.getType()) {
                case SqlBaseLexer.PARSED:
                    type = Explain.Type.PARSED;
                    break;
                case SqlBaseLexer.ANALYZED:
                    type = Explain.Type.ANALYZED;
                    break;
                case SqlBaseLexer.OPTIMIZED:
                    type = Explain.Type.OPTIMIZED;
                    break;
                case SqlBaseLexer.MAPPED:
                    type = Explain.Type.MAPPED;
                    break;
                case SqlBaseLexer.EXECUTABLE:
                    type = Explain.Type.EXECUTABLE;
                    break;
                default:
                    type = Explain.Type.ALL;
            }
        }
        Explain.Format format = (ctx.format != null && ctx.format.getType() == SqlBaseLexer.GRAPHVIZ ? Explain.Format.GRAPHVIZ : Explain.Format.TEXT);
        boolean verify = (ctx.verify != null ? Booleans.parseBoolean(ctx.verify.getText().toLowerCase(Locale.ROOT), true) : true);

        return new Explain(loc, plan(ctx.statement()), type, format, verify);
    }


    @Override
    public Object visitShowFunctions(ShowFunctionsContext ctx) {
        return new ShowFunctions(source(ctx), text(ctx.pattern));
    }

    @Override
    public Object visitShowTables(ShowTablesContext ctx) {
        return new ShowTables(source(ctx), visitIdentifier(ctx.index), text(ctx.pattern));
    }

    @Override
    public Object visitShowSchemas(ShowSchemasContext ctx) {
        return new ShowSchemas(source(ctx));
    }


    @Override
    public Object visitShowColumns(ShowColumnsContext ctx) {
        TableIdentifier identifier = visitTableIdentifier(ctx.tableIdentifier());
        if (!identifier.hasType()) {
            throw new ParsingException(identifier.location(), "Target type needs to be specified");
        }
        return new ShowColumns(source(ctx), identifier.index(), identifier.type());
    }

    @Override
    public Object visitShowSession(ShowSessionContext ctx) {
        return new ShowSession(source(ctx), visitIdentifier(ctx.key), text(ctx.pattern));
    }

    @Override
    public Object visitSessionSet(SessionSetContext ctx) {
        return new SessionSet(source(ctx), visitIdentifier(ctx.key), text(ctx.value));
    }

    @Override
    public Object visitSessionReset(SessionResetContext ctx) {
        return new SessionReset(source(ctx), visitIdentifier(ctx.key), text(ctx.pattern));
    }
}