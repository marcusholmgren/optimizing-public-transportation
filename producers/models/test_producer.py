import pytest


from .producer import Producer


def test_sanitize_station_name():
    assert Producer.sanitize_station_name("O'Hare-bound") == "ohare_bound"
    assert Producer.sanitize_station_name("Terminal arrival") == "terminal_arrival"
    assert Producer.sanitize_station_name("Forest Pk Branch") == "forest_pk_branch"
