from constants import START_TIME, CONDITIONS, RESULT_FILE, END_TIME
from task_api.api_client import YandexWeatherAPI
from task_api.models import (
    CityModel, CityWeatherDayModel, FinalResultsModel, RatingCityModel,
    RatingCityListModel, DayTempConditionModel
)
from utils import logger


class DataFetchingTask:
    """Get weather from API"""

    YandexWeatherAPI = YandexWeatherAPI()

    def make_request(self, city_name: str) -> CityModel:
        """Make request for get data from API"""

        logger.info(
            'Making request to Yandex Weather API for city: %s', city_name
        )
        city_data = self.YandexWeatherAPI.get_forecasting(city_name)

        logger.debug('API response to: %s', city_data)
        return CityModel(city=city_name, forecasts=city_data)


class DataCalculationTask:
    """Calculating weather temp and conditions"""

    @staticmethod
    def calculating_average_temp(temps_list: list) -> float:
        """Calculating average temp with round."""

        if not temps_list:
            return 0
        return round(sum(temps_list) / len(temps_list), 1)

    def calculating_data(self, city_data: CityModel) -> CityWeatherDayModel:
        """Calculating hours conditions and make temp lists"""

        logger.info('Calculating weather for city: %s', city_data.city)
        weather_data = []
        for day in city_data.forecasts.forecasts:
            result_temps_for_day = []
            good_conditions_hours = 0
            for hour in day.hours:
                if START_TIME < hour.hour < END_TIME:
                    result_temps_for_day.append(hour.temp)
                    if hour.condition in CONDITIONS:
                        good_conditions_hours += 1

            logger.debug(
                'Write weather_data to DayTempConditionModel for city: %s',
                city_data.city
            )
            weather_data.append(
                DayTempConditionModel(
                    date=day.date,
                    day_average_temp=self.calculating_average_temp(
                        result_temps_for_day
                    ),
                    good_conditions_hours=good_conditions_hours
                ))

        logger.debug(
            'Write weather_data to CityWeatherDayModel for city: %s',
            city_data.city
        )
        return CityWeatherDayModel(city=city_data.city, data=weather_data)

    def calculating_final_results(
            self, data: CityWeatherDayModel) -> FinalResultsModel:
        """Calculating final weather results for all days"""

        result_average_temps_for_days = []
        total_good_conditions_hours = 0

        logger.debug(
            'Calculation averages temps and conditions for city: %s', data.city
        )
        for value in data.data:
            if value.day_average_temp != 0.0:
                result_average_temps_for_days.append(value.day_average_temp)
                total_good_conditions_hours += value.good_conditions_hours

        logger.debug(
            'Write total weather data to FinalResultsModel for city: %s',
            data.city
        )
        return FinalResultsModel(
            city=data.city,
            total_average_temp=self.calculating_average_temp(
                result_average_temps_for_days
            ),
            total_good_conditions_hours=total_good_conditions_hours,
        )

    @staticmethod
    def adding_rating(data: FinalResultsModel) -> RatingCityListModel:
        """Calculating and add rating for cities"""

        logger.debug('Sorting FinalResultsModel by conditions')
        sorted_cities = sorted(
            data, key=lambda cond: (
                cond.total_average_temp, cond.total_good_conditions_hours
            ), reverse=True,
        )

        logger.debug('Add rating to RatingCityModel')
        rating_cities = []
        for index, city in enumerate(sorted_cities):
            rating_cities.append(RatingCityModel(city=city, rating=index + 1))

        logger.debug('Add RatingCityModel to RatingCityListModel')
        result = RatingCityListModel(cities=rating_cities)
        return result

    def general_calculation(self, city_data: CityModel) -> FinalResultsModel:
        """General calculation data to FinalResultsModel"""

        logger.info(
            'Get calculated weather by day for city: %s', city_data.city
        )
        calculating = self.calculating_data(city_data)

        logger.info('Get final results weather for city: %s', city_data.city)
        result = self.calculating_final_results(calculating)

        logger.debug(
            'Return the FinalResultsModel for city: %s', city_data.city
        )
        return result


class DataAggregationTask:
    """Save final result to json"""

    def __init__(self, lock):
        self.__lock = lock

    @staticmethod
    def save_results_to_json(data: RatingCityListModel) -> None:
        """Save final result to json"""

        logger.debug('Save final result to json')
        with open(RESULT_FILE, 'w', encoding='utf-8') as file:
            file.write(data.to_json())


class DataAnalyzingTask:
    """Get the best city by conditions in stdout"""

    @staticmethod
    def get_result(data: RatingCityListModel) -> str:
        """Get the best city by conditions"""

        logger.info('Finding the best city by temp and weather conditions')
        for rating in data.cities:
            if rating.rating == 1:
                return (f'The best weather condition in city '
                        f'{rating.city.city}: average temperature '
                        f'{rating.city.total_average_temp} and '
                        f'{rating.city.total_good_conditions_hours} hours '
                        f'without bad weather conditions.'
                        )
