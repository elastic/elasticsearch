package org.elasticsearch.river.log4j;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class Log4JRiverModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(River.class).to(Log4JRiver.class).asEagerSingleton();
	}

}
