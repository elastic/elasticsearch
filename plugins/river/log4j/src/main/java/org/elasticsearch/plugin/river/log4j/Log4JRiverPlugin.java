package org.elasticsearch.plugin.river.log4j;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.log4j.Log4JRiverModule;

public class Log4JRiverPlugin extends AbstractPlugin {

	@Inject public Log4JRiverPlugin() {}
	
	@Override
	public String description() {
		return "Log4j River Plugin";
	}

	@Override
	public String name() {
		return "river-log4j";
	}
	
	@Override public void processModule(Module module) {
        if (module instanceof RiversModule) {
            ((RiversModule) module).registerRiver("log4j", Log4JRiverModule.class);
        }
    }

}
