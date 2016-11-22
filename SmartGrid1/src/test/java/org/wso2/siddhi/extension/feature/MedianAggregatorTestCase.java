package org.wso2.siddhi.extension.feature;

/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.concurrent.atomic.AtomicInteger;

public class MedianAggregatorTestCase {

    private static final Logger log = Logger.getLogger(MedianAggregatorTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private volatile boolean eventArrived;
    private static int ccc =1;
    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
    }

    @Test
    public void testSplitFunctionExtension() throws InterruptedException {
        log.info("MedianAggregatorTestCase TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        //siddhiManager.setExtension("feature:feature", MedianAggregator.class);
        String inStreamDefinition = "define stream inputStream (tt double); define stream outputStream (tt double); define stream filteredOutputStream (tt double);";
        //String query ="@info(name = 'query1') " + "from inputStream#window.length(5) " + "select medi
        // an:feature(tt) as tt insert into outputStream;";
        //String query ="@info(name = 'query1') " + "from inputStream#window.length(5) " + "select feature:med(tt) as tt insert into outputStream; from outputStream[tt != -1.0d] select tt as tt insert into filteredOutputStream";
        String query ="@info(name = 'query1') " + "from inputStream#window.length(5) " + "select feature:med(tt) as tt insert into outputStream";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                // EventPrinter.print(events);
                for(Event ev : events){
                    System.out.println("-----------"+ccc+"-----------" + ev.getData()[0]);
                    ccc++;

                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{8.94775});
        // Thread.sleep(1000);
        inputHandler.send(new Object[]{8.68211});
        //Thread.sleep(1000);
        inputHandler.send(new Object[]{8.44443});
        //Thread.sleep(1000);
        inputHandler.send(new Object[]{8.23472});
        //Thread.sleep(1000);
        inputHandler.send(new Object[]{10.9959});
        //Thread.sleep(100);
        inputHandler.send(new Object[]{10.3738});
        //Thread.sleep(100);
        inputHandler.send(new Object[]{9.76563});
        //Thread.sleep(100);
        inputHandler.send(new Object[]{9.17144});
        //Thread.sleep(100);
        inputHandler.send(new Object[]{8.19278});
        //Thread.sleep(100);
        inputHandler.send(new Object[]{7.49374});

        /*inputHandler.send(new Object[]{10.2899});
        inputHandler.send(new Object[]{10.982});
        inputHandler.send(new Object[]{11.4363});
        inputHandler.send(new Object[]{11.653});
        inputHandler.send(new Object[]{11.6321});
        inputHandler.send(new Object[]{10.9959});
        inputHandler.send(new Object[]{10.3738});
        inputHandler.send(new Object[]{9.76563});
        inputHandler.send(new Object[]{9.17144});
        inputHandler.send(new Object[]{8.19278});
        inputHandler.send(new Object[]{7.49374});
        inputHandler.send(new Object[]{7.07431});

 /*       BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader("/home/fathima/GLD/data/testDataQcep.csv"));
            // read file line by line
            String line = null;
            Scanner scanner = null;
            int index = 0;
            double windspeed = 0.0;
            while ((line = reader.readLine()) != null) {

                scanner = new Scanner(line);
                scanner.useDelimiter(",");
                while (scanner.hasNext()) {
                    String data = scanner.next();
                    if (index == 2) {
                        windspeed = Double.parseDouble(data);

                    }
                    index++;
                }
                index = 0;
                inputHandler.send(new Object[]{windspeed});
            }

            //close reader
            reader.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
*/

        Thread.sleep(2000);

        executionPlanRuntime.shutdown();
    }

}
