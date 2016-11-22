package org.wso2.siddhi.extension.feature;/*
* Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
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
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;


public class MedianAggregator extends  AttributeAggregator{
    private MedianAggregator meadianAgg;
    Logger log = Logger.getLogger(MedianAggregator.class);

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param executionPlanContext         Execution plan runtime context
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("Median aggregator has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length + " parameters provided");
        }

        Attribute.Type type = attributeExpressionExecutors[0].getReturnType();

        switch (type) {

            case DOUBLE:
                meadianAgg = new MedianAggregatorDouble();
                break;
            default:
                throw new OperationNotSupportedException("Median not supported for " + type);
        }
    }

    @Override
    public Attribute.Type getReturnType() {
        return meadianAgg.getReturnType();
    }

    @Override
    public Object processAdd(Object data) {
        return meadianAgg.processAdd(data);
    }

    @Override
    public Object processAdd(Object[] data) {
        return new IllegalStateException("Median cannot process data array, but found " + Arrays.deepToString(data));
    }

    @Override
    public Object processRemove(Object data) {
        return meadianAgg.processRemove(data);
    }

    @Override
    public Object processRemove(Object[] data) {
        return new IllegalStateException("Median cannot process data array, but found " + Arrays.deepToString(data));
    }

    @Override
    public Object reset() {
        return meadianAgg.reset();
    }

    @Override
    public Object[] currentState() {
        return meadianAgg.currentState();
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void restoreState(Object[] state) {
        meadianAgg.restoreState(state);
    }

    private class MedianAggregatorDouble extends MedianAggregator {
        private final Attribute.Type type = Attribute.Type.DOUBLE;
        ArrayList<Double> arr = new ArrayList<Double>();
        private double mean, oldMean, stdDeviation, sum, median;
        private int count = 0;

        @Override
        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {

            count++;
            //System.out.println("process add :"+count);
            arr.add((Double)data);



            /*
            if(arr.size()==5){
                double feature = getMedian(arr);
                System.out.println("1 :"+arr);
                return feature;
            } else if(arr.size()>5){
                arr.remove(0);
                double feature = getMedian(arr);
                System.out.println("2 :"+arr);
                return feature;
            }*/
            if(arr.size()==5){
                double median = getMedian(arr);
                //System.out.println("Median Value : "+feature);
                return median;
            }
            return 0.0;
        }

        private double getMedian(ArrayList<Double> test) {
            ArrayList<Double> arrSorted = new ArrayList<Double>();
            for(int i =0; i<test.size(); i++){
                arrSorted.add(test.get(i));

            }

            Collections.sort(arrSorted);
            ;
            int pointA = arrSorted.size()/2;
            if(arrSorted.size()%2==0){
                int pointB = pointA-1;
                return (arrSorted.get(pointA)+arrSorted.get(pointB))/2;
            }


            return arrSorted.get(pointA);
        }

        @Override
        public Object processRemove(Object data) {
           // System.out.println("process remove :"+count);
            arr.remove(0);
            return 0.0;
        }

        @Override
        public Object reset() {

            return 0.0;
        }

        @Override
        public Object[] currentState() {
            return new Object[]{};
        }

        @Override
        public void restoreState(Object[] state) {

        }
    }


}

