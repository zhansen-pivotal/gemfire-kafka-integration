package io.pivotal.gemfire.samples.conf;


import io.pivotal.gemfire.samples.integration.function.LoadPersonDataFunction;
import io.pivotal.gemfire.samples.integration.writer.KafkaIntegrationWriter;
import io.pivotal.gemfire.samples.model.key.PersonKey;
import io.pivotal.gemfire.samples.model.pdx.Person;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.gemfire.CacheFactoryBean;
import org.springframework.data.gemfire.PartitionedRegionFactoryBean;
import org.springframework.data.gemfire.function.FunctionServiceFactoryBean;

import java.util.Arrays;
import java.util.Properties;


@Configuration
public class KafkaIntegrationGemfireConfig {

    @Value("${embedded-locator-host:localhost}")
    private String embeddedLocatorHost;

    @Value("${embedded-locator-port:10334}")
    private int embeddedLocatorPort;

    @Value("${cache-server.port:40404}")
    private int port;


    @Bean
    Properties geodeProperties() {
        Properties geodeProperties = new Properties();
        geodeProperties.setProperty("name", "KafkaIntegrationGemfire");
        geodeProperties.setProperty("start-locator", embeddedLocatorHost + "[" + embeddedLocatorPort + "]");
        geodeProperties.setProperty("jmx-manager", "true");
        return geodeProperties;
    }

    @Bean
    CacheFactoryBean geodeCache() {
        CacheFactoryBean geodeCache = new CacheFactoryBean();
        geodeCache.setClose(true);
        geodeCache.setPdxSerializer(new ReflectionBasedAutoSerializer("io.pivotal.gemfire.samples.model.pdx.*"));
        geodeCache.setProperties(geodeProperties());
        return geodeCache;
    }

    @Bean("person")
    PartitionedRegionFactoryBean<PersonKey, Person> personRegion(final Cache geodeCache) {
        PartitionedRegionFactoryBean<PersonKey, Person> personRegion = new PartitionedRegionFactoryBean<>();
        personRegion.setCache(geodeCache);
        personRegion.setClose(false);
        personRegion.setName("person");
        personRegion.setCacheWriter(personCacheWriter());
        return personRegion;
    }

    @Bean
    CacheWriter<PersonKey, Person> personCacheWriter() {
        return new KafkaIntegrationWriter();
    }

    @Bean
    FunctionServiceFactoryBean functionService(final Function loadPersonDataFunction) {
        FunctionServiceFactoryBean functionService = new FunctionServiceFactoryBean();
        functionService
                .setFunctions(Arrays.asList(new Function[]{loadPersonDataFunction}));
        return functionService;
    }

    @Bean
    Function loadPersonDataFunction() {
        LoadPersonDataFunction loadPersonDataFunction = new LoadPersonDataFunction();
        return loadPersonDataFunction;
    }

}
