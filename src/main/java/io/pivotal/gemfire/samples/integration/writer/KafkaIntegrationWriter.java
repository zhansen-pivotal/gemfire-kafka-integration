package io.pivotal.gemfire.samples.integration.writer;


import io.pivotal.gemfire.samples.model.key.PersonKey;
import io.pivotal.gemfire.samples.model.pdx.Person;
import org.apache.geode.cache.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import javax.annotation.Resource;


public class KafkaIntegrationWriter implements CacheWriter<PersonKey, Person> {

    @Autowired
    KafkaTemplate kafkaTemplate;


    @Override
    public void beforeUpdate(EntryEvent<PersonKey, Person> event) throws CacheWriterException {
        PersonKey personKey = event.getKey();
        Person personOld = event.getOldValue();
        Person personNew = event.getNewValue();

        if (personOld != null) { //should be used for something like timestamp for conflict resolution
            System.out.println("This is an update. Sending new data to kafka");
            kafkaTemplate.sendDefault(personKey.toString(), personNew.toString());
        }
    }

    @Override
    public void beforeCreate(EntryEvent<PersonKey, Person> event) throws CacheWriterException {
        PersonKey personKey = event.getKey();
        Person person = event.getNewValue();
        kafkaTemplate.send("person", personKey.toString(), person.toString());
    }

    @Override
    public void beforeDestroy(EntryEvent<PersonKey, Person> event) throws CacheWriterException {

    }

    @Override
    public void beforeRegionDestroy(RegionEvent<PersonKey, Person> event) throws CacheWriterException {

    }

    @Override
    public void beforeRegionClear(RegionEvent<PersonKey, Person> event) throws CacheWriterException {

    }

    @Override
    public void close() {

    }
}
