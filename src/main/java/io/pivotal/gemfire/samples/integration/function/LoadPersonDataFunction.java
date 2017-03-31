package io.pivotal.gemfire.samples.integration.function;

import io.pivotal.gemfire.samples.model.key.PersonKey;
import io.pivotal.gemfire.samples.model.pdx.Person;
import io.pivotal.gemfire.samples.util.RandomPersonBuilder;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhansen on 3/31/17.
 */
public class LoadPersonDataFunction implements Function, Declarable {

    @Resource(name = "person")
    Region<PersonKey, Person> personRegion;


    @Override
    public void init(Properties properties) {

    }

    @Override
    public boolean hasResult() {
        return true;
    }

    @Override
    public void execute(FunctionContext functionContext) {
        ResultSender resultSender = functionContext.getResultSender();
        RandomPersonBuilder randomPersonBuilder = new RandomPersonBuilder();
        Map personList = randomPersonBuilder.buildPerson(1000, 1000); //builds a map of people
        personRegion.putAll(personList); //writes person objects to gemfire
        resultSender.lastResult(personList.size()); //return the last result to signal function end
    }

    @Override
    public String getId() {
        return getClass().getSimpleName();
    }

    @Override
    public boolean optimizeForWrite() {
        return true;
    }

    @Override
    public boolean isHA() {
        return true;
    }
}
