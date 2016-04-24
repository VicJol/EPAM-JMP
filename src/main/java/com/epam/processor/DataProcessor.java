package com.epam.processor;

import com.epam.data.RoadAccident;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This is to be completed by mentees
 */
public class DataProcessor {

    private final List<RoadAccident> roadAccidentList;

    public DataProcessor(List<RoadAccident> roadAccidentList){
        this.roadAccidentList = roadAccidentList;
    }


//    First try to solve task using java 7 style for processing collections

    /**
     * Return road accident with matching index
     * @param index
     * @return
     */
    public RoadAccident getAccidentByIndex7(String index){
        for(RoadAccident accident:roadAccidentList ){
            if(accident.getAccidentId().equals(index)){
                return accident;
            }
        }
        return null;
    }


    /**
     * filter list by longtitude and latitude values, including boundaries
     * @param minLongitude
     * @param maxLongitude
     * @param minLatitude
     * @param maxLatitude
     * @return
     */
    public Collection<RoadAccident> getAccidentsByLocation7(float minLongitude, float maxLongitude, float minLatitude, float maxLatitude){
        List<RoadAccident> roadAccidentCollection = new ArrayList() ;
        for(RoadAccident accident : roadAccidentList){
            if( accident.getLongitude()>=minLongitude &&
                    accident.getLongitude()<=maxLongitude &&
                    accident.getLatitude()>=minLatitude &&
                    accident.getLatitude()<=maxLatitude){
                roadAccidentCollection.add(accident);
            }
        }

        return roadAccidentCollection;
    }

    /**
     * count incidents by road surface conditions
     * ex:
     * wet -> 2
     * dry -> 5
     * @return
     */
    public Map<String, Long> getCountByRoadSurfaceCondition7(){
        Map<String, Long> countByRoadSurface = new HashMap<String, Long>() ;
        for(RoadAccident accident : roadAccidentList){
            String roadSurfaceCondition = accident.getRoadSurfaceConditions();
            Long number = countByRoadSurface.get(roadSurfaceCondition);
            /*if(number!=null){
                number ++;
            }else{
                number = 1L;
            }*/
            countByRoadSurface.put(roadSurfaceCondition, number==null?1L:(number+1L));
        }
        return countByRoadSurface;
    }

    /**
     * find the weather conditions which caused the top 3 number of incidents
     * as example if there were 10 accidence in rain, 5 in snow, 6 in sunny and 1 in foggy, then your result list should contain {rain, sunny, snow} - top three in decreasing order
     * @return
     */
    public List<String> getTopThreeWeatherCondition7(){
        TreeMap<String, Long> weatherConditonIncidentCountsMap = new TreeMap<String, Long>() ;
        for(RoadAccident accident : roadAccidentList){
            String weatherCondition = accident.getWeatherConditions();
            Long number = weatherConditonIncidentCountsMap.get(weatherCondition);
            weatherConditonIncidentCountsMap.put(weatherCondition, number==null?1L:(number+1L));
        }

        SortedSet<Map.Entry<String, Long>> SortedSet = new TreeSet<Map.Entry<String, Long>>(
                new Comparator<Map.Entry<String, Long>>(){
                    @Override
                    public int compare(Map.Entry<String, Long> e1, Map.Entry<String, Long> e2){
                        return e2.getValue().compareTo(e1.getValue());
                    }
                }
        );

        List<String> topThreeList = new ArrayList<String>();
        SortedSet.addAll(weatherConditonIncidentCountsMap.entrySet());

        for(Map.Entry<String, Long> entry: SortedSet) {
            System.out.println("The reverse tree key is: " + entry.getKey() + " And the value is:" + entry.getValue());
        }

        int i=0;
        for(Map.Entry<String, Long> entry: SortedSet){
            if(i<3) {
                topThreeList.add(entry.getKey());
                i=i+1;
            }
            else{
                break;
            }
        }

        return topThreeList;

    }

    /**
     * return a multimap where key is a district authority and values are accident ids
     * ex:
     * authority1 -> id1, id2, id3
     * authority2 -> id4, id5
     * @return
     */
    public Multimap<String, String> getAccidentIdsGroupedByAuthority7(){
        Multimap<String, String> authorityAndIdsMultimap = ArrayListMultimap.create();
        for(RoadAccident accident : roadAccidentList) {
            authorityAndIdsMultimap.put(accident.getDistrictAuthority(), accident.getAccidentId());
        }
        return authorityAndIdsMultimap;
    }



    // Now let's do same tasks but now with streaming api



    public RoadAccident getAccidentByIndex(String index){
        return roadAccidentList.stream()
                .filter(accident -> accident.getAccidentId().equals(index))
                .findFirst().get();
    }


    /**
     * filter list by longtitude and latitude fields
     * @param minLongitude
     * @param maxLongitude
     * @param minLatitude
     * @param maxLatitude
     * @return
     */
    public Collection<RoadAccident> getAccidentsByLocation(float minLongitude, float maxLongitude, float minLatitude, float maxLatitude){
        return roadAccidentList.stream()
                .filter(accident -> accident.getLongitude()>=minLongitude &&
                        accident.getLongitude()<=maxLongitude &&
                        accident.getLatitude()>=minLatitude &&
                        accident.getLatitude()<=maxLatitude)
                .collect(Collectors.toList());
    }

    /**
     * find the weather conditions which caused max number of incidents
     * @return
     */
    public List<String> getTopThreeWeatherCondition(){
        Map<String, Long> countByRoadSurface = roadAccidentList.stream()
                   								.map(RoadAccident::getWeatherConditions)
                   								.collect(Collectors.groupingBy(
                   										item -> item,
                                                        Collectors.counting()
                           								));

        return countByRoadSurface.entrySet().stream()
                   					.sorted((val1,val2) -> val2.getValue().compareTo(val1.getValue()))
                   					.map(Map.Entry::getKey)
                   					.limit(3)
                   					.collect(Collectors.toList());
    }

    /**
     * count incidents by road surface conditions
     * @return
     */
    public Map<String, Long> getCountByRoadSurfaceCondition(){
        Map<String, Long> countByRoadSurface
                = roadAccidentList.stream()
                .collect(Collectors.groupingBy(RoadAccident::getRoadSurfaceConditions, Collectors.counting()));
        return countByRoadSurface;
    }

    /**
     * To match streaming operations result, return type is a java collection instead of multimap
     * @return
     */
    public Map<String, List<String>> getAccidentIdsGroupedByAuthority(){
        Map<String, List<String>> authorityAndIdsMmap = roadAccidentList.stream()
                .collect(
                        Collectors.groupingBy(
                                RoadAccident::getDistrictAuthority,
                                Collectors.mapping(RoadAccident::getAccidentId, Collectors.toList())));

        return authorityAndIdsMmap;
    }

}



