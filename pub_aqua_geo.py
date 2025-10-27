"""Utilities for fetching Pub-Aqua site border latitude/longitude data."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple, Union

import requests

ResponseParser = Callable[[requests.Response], List[Dict[str, Any]]]


class SiteBorderClient:
    """Client responsible for retrieving border metadata for a site."""

    def __init__(
        self,
        *,
        session: requests.Session,
        base_url: str,
        timeout: Tuple[int, int],
        parse_response: ResponseParser,
    ) -> None:
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._parse_response = parse_response

    def fetch_site_borders(self, site_nr: Union[int, str]) -> List[Dict[str, Any]]:
        """Fetch site border metadata (including latitude/longitude points)."""

        url = f"{self._base_url}/api/v1/sites/{site_nr}/borders"
        response = self._session.get(url, timeout=self._timeout)
        return self._parse_response(response)

    def fetch_site_border_points(self, site_nr: Union[int, str]) -> List[Tuple[float, float]]:
        """Return a flat list of ``(latitude, longitude)`` tuples for a site's borders."""

        borders = self.fetch_site_borders(site_nr)
        points: List[Tuple[float, float]] = []
        for border in borders:
            for point in border.get("points", []):
                latitude = point.get("latitude")
                longitude = point.get("longitude")
                if latitude is None or longitude is None:
                    continue
                points.append((latitude, longitude))
        return points
