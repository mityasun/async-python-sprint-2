import json

from pydantic import BaseModel


class HoursModel(BaseModel):
    """Weather data for one hour"""

    hour: int
    temp: int
    condition: str


class DatesModel(BaseModel):
    """Weather data by days with data by hours"""

    date: str
    hours: list[HoursModel]


class ListDaysModel(BaseModel):
    """Weather data by days with data by hours"""

    forecasts: list[DatesModel]


class CityModel(BaseModel):
    """Weather data by city with """

    city: str
    forecasts: ListDaysModel


class DayTempConditionModel(BaseModel):
    """Average weather data by date"""

    date: str
    day_average_temp: float
    good_conditions_hours: int


class CityWeatherDayModel(BaseModel):
    """Model with total average results by city"""
    city: str
    data: list[DayTempConditionModel]


class FinalResultsModel(BaseModel):
    """Final model with total average results for cities"""

    city: str
    total_average_temp: float
    total_good_conditions_hours: int


class RatingCityModel(BaseModel):
    """Rating model by city"""

    city: FinalResultsModel
    rating: int


class RatingCityListModel(BaseModel):
    """Rating list for cities"""

    cities: list[RatingCityModel]

    def to_json(self):
        return json.dumps(
            self, default=lambda o: o.__dict__, sort_keys=True, indent=4
        )
